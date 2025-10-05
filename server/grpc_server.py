"""
gRPC server implementation for Raft and Client services.
"""

import asyncio
import logging
from typing import Optional

import grpc
from concurrent import futures

from raft.proto import raft_pb2, raft_pb2_grpc
from client.proto import client_pb2, client_pb2_grpc

logger = logging.getLogger(__name__)


class RaftService(raft_pb2_grpc.RaftServiceServicer):
    """gRPC service implementation for Raft RPCs."""
    
    def __init__(self, raft_node):
        """
        Initialize Raft service.
        
        Args:
            raft_node: RaftNode instance to delegate calls to
        """
        self.raft_node = raft_node
    
    async def RequestVote(self, request, context):
        """Handle RequestVote RPC."""
        logger.debug(f"Received RequestVote from {request.candidate_id}")
        
        try:
            response = await self.raft_node.handle_request_vote(request)
            return response
        except Exception as e:
            logger.error(f"Error handling RequestVote: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return raft_pb2.RequestVoteResponse(term=0, vote_granted=False)
    
    async def AppendEntries(self, request, context):
        """Handle AppendEntries RPC."""
        logger.debug(f"Received AppendEntries from {request.leader_id}")
        
        try:
            response = await self.raft_node.handle_append_entries(request)
            return response
        except Exception as e:
            logger.error(f"Error handling AppendEntries: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return raft_pb2.AppendEntriesResponse(term=0, success=False, match_index=0)


class ClientService(client_pb2_grpc.ClientServiceServicer):
    """gRPC service implementation for Client API."""
    
    def __init__(self, raft_node):
        """
        Initialize Client service.
        
        Args:
            raft_node: RaftNode instance to delegate calls to
        """
        self.raft_node = raft_node
    
    async def PutPrice(self, request, context):
        """Handle PutPrice RPC."""
        logger.debug(f"Received PutPrice: {request.ticker_price.symbol}={request.ticker_price.price}")
        
        try:
            result = await self.raft_node.put_price(
                request.ticker_price.symbol,
                request.ticker_price.price,
                request.ticker_price.timestamp
            )
            
            response = client_pb2.PutPriceResponse(
                ok=result["ok"],
                leader_hint=result["leader_hint"] or "",
                error_message=result["error_message"] or ""
            )
            return response
            
        except Exception as e:
            logger.error(f"Error handling PutPrice: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return client_pb2.PutPriceResponse(
                ok=False,
                leader_hint="",
                error_message=str(e)
            )
    
    async def BatchPutPrice(self, request, context):
        """Handle BatchPutPrice RPC."""
        logger.debug(f"Received BatchPutPrice with {len(request.ticker_prices)} prices")
        
        try:
            # For Week 1, just return NOT_LEADER
            if self.raft_node.state.value != "leader":
                return client_pb2.BatchPutPriceResponse(
                    ok=False,
                    leader_hint="",
                    error_message="Not leader"
                )
            
            # TODO: In Week 2, implement batch processing
            response = client_pb2.BatchPutPriceResponse(
                ok=True,
                leader_hint=self.raft_node.node_id,
                error_message=""
            )
            return response
            
        except Exception as e:
            logger.error(f"Error handling BatchPutPrice: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return client_pb2.BatchPutPriceResponse(
                ok=False,
                leader_hint="",
                error_message=str(e)
            )
    
    async def GetPrice(self, request, context):
        """Handle GetPrice RPC."""
        logger.debug(f"Received GetPrice: {request.symbol}")
        
        try:
            result = await self.raft_node.get_price(request.symbol)
            
            if result["found"]:
                ticker_price = client_pb2.TickerPrice(
                    symbol=request.symbol,
                    price=result["ticker_price"]["price"],
                    timestamp=result["ticker_price"]["timestamp"]
                )
                response = client_pb2.GetPriceResponse(
                    ticker_price=ticker_price,
                    found=True,
                    error_message=""
                )
            else:
                response = client_pb2.GetPriceResponse(
                    ticker_price=None,
                    found=False,
                    error_message=result["error_message"]
                )
            
            return response
            
        except Exception as e:
            logger.error(f"Error handling GetPrice: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return client_pb2.GetPriceResponse(
                ticker_price=None,
                found=False,
                error_message=str(e)
            )
    
    async def GetClusterInfo(self, request, context):
        """Handle GetClusterInfo RPC."""
        logger.debug("Received GetClusterInfo")
        
        try:
            info = await self.raft_node.get_cluster_info()
            
            response = client_pb2.GetClusterInfoResponse(
                leader_id=info["leader_id"] or "",
                term=info["term"],
                members=info["members"],
                node_id=info["node_id"],
                role=info["role"]
            )
            return response
            
        except Exception as e:
            logger.error(f"Error handling GetClusterInfo: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return client_pb2.GetClusterInfoResponse(
                leader_id="",
                term=0,
                members=[],
                node_id="",
                role="unknown"
            )


class GrpcServer:
    """Main gRPC server that hosts both Raft and Client services."""
    
    def __init__(self, raft_node, raft_port: int, client_port: int):
        """
        Initialize gRPC server.
        
        Args:
            raft_node: RaftNode instance
            raft_port: Port for Raft service
            client_port: Port for Client service
        """
        self.raft_node = raft_node
        self.raft_port = raft_port
        self.client_port = client_port
        
        # Create services
        self.raft_service = RaftService(raft_node)
        self.client_service = ClientService(raft_node)
        
        # Server instances
        self.raft_server: Optional[grpc.aio.Server] = None
        self.client_server: Optional[grpc.aio.Server] = None
        
        logger.info(f"gRPC server initialized: raft_port={raft_port}, client_port={client_port}")
    
    async def start(self) -> None:
        """Start both gRPC servers."""
        logger.info("Starting gRPC servers...")
        
        # Start Raft server
        self.raft_server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftServiceServicer_to_server(self.raft_service, self.raft_server)
        
        raft_listen_addr = f'[::]:{self.raft_port}'
        self.raft_server.add_insecure_port(raft_listen_addr)
        await self.raft_server.start()
        logger.info(f"Raft server started on {raft_listen_addr}")
        
        # Start Client server
        self.client_server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))
        client_pb2_grpc.add_ClientServiceServicer_to_server(self.client_service, self.client_server)
        
        client_listen_addr = f'[::]:{self.client_port}'
        self.client_server.add_insecure_port(client_listen_addr)
        await self.client_server.start()
        logger.info(f"Client server started on {client_listen_addr}")
        
        # Set server references in raft node
        self.raft_node.raft_server = self.raft_server
        self.raft_node.client_server = self.client_server
    
    async def stop(self) -> None:
        """Stop both gRPC servers."""
        logger.info("Stopping gRPC servers...")
        
        if self.raft_server:
            await self.raft_server.stop(grace=5.0)
            logger.info("Raft server stopped")
        
        if self.client_server:
            await self.client_server.stop(grace=5.0)
            logger.info("Client server stopped")
    
    async def wait_for_termination(self) -> None:
        """Wait for servers to terminate."""
        if self.raft_server and self.client_server:
            await asyncio.gather(
                self.raft_server.wait_for_termination(),
                self.client_server.wait_for_termination()
            )
