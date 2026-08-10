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

    async def InstallSnapshot(self, request, context):
        """Handle InstallSnapshot RPC."""
        logger.debug(f"Received InstallSnapshot from {request.leader_id}")

        try:
            response = await self.raft_node.handle_install_snapshot(request)
            return response
        except Exception as e:
            logger.error(f"Error handling InstallSnapshot: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return raft_pb2.InstallSnapshotResponse(term=0)


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
            from kv.state_machine import TickerPrice
            ticker_prices = [
                TickerPrice(symbol=tp.symbol, price=tp.price, timestamp=tp.timestamp)
                for tp in request.ticker_prices
            ]

            result = await self.raft_node.batch_put_price(ticker_prices)

            response = client_pb2.BatchPutPriceResponse(
                ok=result["ok"],
                leader_hint=result["leader_hint"] or "",
                error_message=result["error_message"] or ""
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
    
    async def DumpState(self, request, context):
        """Handle DumpState RPC."""
        logger.debug("Received DumpState")
        
        try:
            result = await self.raft_node.dump_state()
            
            if result["ok"]:
                # Convert KV store to protobuf format
                kv_store = []
                for symbol, ticker_price_dict in result["kv_store"].items():
                    kv_store.append(client_pb2.TickerPrice(
                        symbol=symbol,
                        price=ticker_price_dict["price"],
                        timestamp=ticker_price_dict["timestamp"]
                    ))
                
                # Convert metrics to protobuf format. The *_total fields are
                # protobuf uint64 - prometheus_client's Counter/Gauge values
                # are always stored (and returned by ._value.get()) as float
                # internally regardless of what was recorded, so these need
                # an explicit int() or protobuf raises "'float' object cannot
                # be interpreted as an integer" (only surfaced by an actual
                # dump-state call against live, recorded metrics - the old
                # dead collector never had non-zero values to expose this).
                metrics = None
                if result.get("metrics"):
                    m = result["metrics"]
                    metrics = client_pb2.RaftMetrics(
                        elections_total=int(m.get("elections_total", 0)),
                        election_duration_ms=m.get("election_duration_ms", 0.0),
                        entries_replicated_total=int(m.get("entries_replicated_total", 0)),
                        replication_latency_ms=m.get("replication_latency_ms", 0.0),
                        replication_failures_total=int(m.get("replication_failures_total", 0)),
                        commits_total=int(m.get("commits_total", 0)),
                        commit_latency_ms=m.get("commit_latency_ms", 0.0),
                        crash_recoveries_total=int(m.get("crash_recoveries_total", 0)),
                        replay_entries_total=int(m.get("replay_entries_total", 0)),
                        snapshot_load_time_ms=m.get("snapshot_load_time_ms", 0.0),
                        catchup_latency_ms=m.get("catchup_latency_ms", 0.0),
                        log_entries_total=int(m.get("log_entries_total", 0)),
                        storage_writes_total=int(m.get("storage_writes_total", 0)),
                        storage_reads_total=int(m.get("storage_reads_total", 0)),
                        commands_applied_total=int(m.get("commands_applied_total", 0)),
                        kv_entries_total=int(m.get("kv_entries_total", 0))
                    )
                
                response = client_pb2.DumpStateResponse(
                    ok=True,
                    error_message="",
                    node_id=result["node_id"],
                    current_term=result["current_term"],
                    state=result["state"],
                    commit_index=result["commit_index"],
                    last_applied=result["last_applied"],
                    log_length=result["log_length"],
                    kv_entries=result["kv_entries"],
                    kv_store=kv_store,
                    metrics=metrics
                )
            else:
                response = client_pb2.DumpStateResponse(
                    ok=False,
                    error_message=result["error_message"]
                )
            
            return response
            
        except Exception as e:
            logger.error(f"Error handling DumpState: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return client_pb2.DumpStateResponse(
                ok=False,
                error_message=str(e)
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
        
        # Start metrics server
        try:
            from server.metrics_server import start_metrics_server
            from raft.types import metrics_port_for_node_id
            metrics_port = metrics_port_for_node_id(self.raft_node.node_id)
            await start_metrics_server(metrics_port, raft_node=self.raft_node)
            logger.info(f"Metrics server started on port {metrics_port}")
        except Exception as e:
            logger.warning(f"Failed to start metrics server: {e}")
    
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
