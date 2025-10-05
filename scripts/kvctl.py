#!/usr/bin/env python3
"""
Simple CLI tool for interacting with the Raft cluster.
"""

import asyncio
import argparse
import sys
import logging
from typing import Optional

import grpc
from client.proto import client_pb2, client_pb2_grpc

# Setup logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


class RaftClient:
    """Client for interacting with Raft cluster."""
    
    def __init__(self, host: str, port: int):
        """
        Initialize client.
        
        Args:
            host: Server hostname
            port: Server port
        """
        self.host = host
        self.port = port
        self.address = f"{host}:{port}"
    
    async def get_cluster_info(self) -> Optional[dict]:
        """Get cluster information."""
        try:
            async with grpc.aio.insecure_channel(self.address) as channel:
                stub = client_pb2_grpc.ClientServiceStub(channel)
                
                request = client_pb2.GetClusterInfoRequest()
                response = await stub.GetClusterInfo(request)
                
                return {
                    "leader_id": response.leader_id,
                    "term": response.term,
                    "members": list(response.members),
                    "node_id": response.node_id,
                    "role": response.role
                }
                
        except Exception as e:
            logger.error(f"Failed to get cluster info: {e}")
            return None
    
    async def put_price(self, symbol: str, price: float, timestamp: Optional[int] = None) -> Optional[dict]:
        """Put a stock price."""
        if timestamp is None:
            import time
            timestamp = int(time.time() * 1000)  # milliseconds
        
        try:
            async with grpc.aio.insecure_channel(self.address) as channel:
                stub = client_pb2_grpc.ClientServiceStub(channel)
                
                ticker_price = client_pb2.TickerPrice(
                    symbol=symbol,
                    price=price,
                    timestamp=timestamp
                )
                
                request = client_pb2.PutPriceRequest(ticker_price=ticker_price)
                response = await stub.PutPrice(request)
                
                return {
                    "ok": response.ok,
                    "leader_hint": response.leader_hint,
                    "error_message": response.error_message
                }
                
        except Exception as e:
            logger.error(f"Failed to put price: {e}")
            return None
    
    async def get_price(self, symbol: str) -> Optional[dict]:
        """Get a stock price."""
        try:
            async with grpc.aio.insecure_channel(self.address) as channel:
                stub = client_pb2_grpc.ClientServiceStub(channel)
                
                request = client_pb2.GetPriceRequest(symbol=symbol)
                response = await stub.GetPrice(request)
                
                if response.found:
                    return {
                        "found": True,
                        "symbol": response.ticker_price.symbol,
                        "price": response.ticker_price.price,
                        "timestamp": response.ticker_price.timestamp
                    }
                else:
                    return {
                        "found": False,
                        "error_message": response.error_message
                    }
                
        except Exception as e:
            logger.error(f"Failed to get price: {e}")
            return None
    
    async def dump_state(self) -> Optional[dict]:
        """Dump local node state."""
        try:
            async with grpc.aio.insecure_channel(self.address) as channel:
                stub = client_pb2_grpc.ClientServiceStub(channel)
                
                request = client_pb2.DumpStateRequest()
                response = await stub.DumpState(request)
                
                if response.ok:
                    kv_store = []
                    for entry in response.kv_store:
                        kv_store.append({
                            "symbol": entry.symbol,
                            "price": entry.price,
                            "timestamp": entry.timestamp
                        })
                    
                    return {
                        "ok": True,
                        "node_id": response.node_id,
                        "current_term": response.current_term,
                        "state": response.state,
                        "commit_index": response.commit_index,
                        "last_applied": response.last_applied,
                        "log_length": response.log_length,
                        "kv_entries": response.kv_entries,
                        "kv_store": kv_store,
                        "metrics": {
                            "elections_total": response.metrics.elections_total,
                            "entries_replicated_total": response.metrics.entries_replicated_total,
                            "commits_total": response.metrics.commits_total,
                            "crash_recoveries_total": response.metrics.crash_recoveries_total,
                            "replay_entries_total": response.metrics.replay_entries_total,
                            "storage_writes_total": response.metrics.storage_writes_total,
                            "commands_applied_total": response.metrics.commands_applied_total
                        } if response.metrics else None
                    }
                else:
                    return {
                        "ok": False,
                        "error_message": response.error_message
                    }
                
        except Exception as e:
            logger.error(f"Failed to dump state: {e}")
            return None


async def cmd_cluster_info(args):
    """Handle cluster-info command."""
    client = RaftClient(args.host, args.port)
    
    print(f"Querying cluster info from {client.address}...")
    
    info = await client.get_cluster_info()
    if info is None:
        print("Failed to get cluster info")
        return 1
    
    print(f"Node ID: {info['node_id']}")
    print(f"Role: {info['role']}")
    print(f"Current Term: {info['term']}")
    print(f"Leader ID: {info['leader_id'] or 'None'}")
    print(f"Members: {', '.join(info['members'])}")
    
    return 0


async def cmd_put_price(args):
    """Handle put-price command."""
    client = RaftClient(args.host, args.port)
    
    print(f"Putting price {args.symbol}={args.price} to {client.address}...")
    
    result = await client.put_price(args.symbol, args.price)
    if result is None:
        print("Failed to put price")
        return 1
    
    if result["ok"]:
        print(f"Successfully put {args.symbol}={args.price}")
    else:
        print(f"Failed to put price: {result['error_message']}")
        if result["leader_hint"]:
            print(f"Try connecting to leader: {result['leader_hint']}")
    
    return 0


async def cmd_get_price(args):
    """Handle get-price command."""
    client = RaftClient(args.host, args.port)
    
    print(f"Getting price for {args.symbol} from {client.address}...")
    
    result = await client.get_price(args.symbol)
    if result is None:
        print("Failed to get price")
        return 1
    
    if result["found"]:
        print(f"{result['symbol']}: ${result['price']} (timestamp: {result['timestamp']})")
    else:
        print(f"Price not found: {result['error_message']}")
    
    return 0


async def cmd_dump_state(args):
    """Handle dump-state command."""
    client = RaftClient(args.host, args.port)
    
    print(f"Dumping state from {client.address}...")
    
    result = await client.dump_state()
    if result is None:
        print("Failed to dump state")
        return 1
    
    if result["ok"]:
        print("=== Node State Dump ===")
        print(f"Node ID: {result['node_id']}")
        print(f"Current Term: {result['current_term']}")
        print(f"State: {result['state']}")
        print(f"Commit Index: {result['commit_index']}")
        print(f"Last Applied: {result['last_applied']}")
        print(f"Log Length: {result['log_length']}")
        print(f"KV Entries: {result['kv_entries']}")
        print()
        
        if result['kv_entries'] > 0:
            print("=== KV Store Contents ===")
            for entry in result['kv_store']:
                print(f"  {entry['symbol']}: ${entry['price']:.2f} (timestamp: {entry['timestamp']})")
            print()
        
        if result.get('metrics'):
            print("=== Metrics ===")
            metrics = result['metrics']
            print(f"  Elections: {metrics.get('elections_total', 0)}")
            print(f"  Replicated Entries: {metrics.get('entries_replicated_total', 0)}")
            print(f"  Commits: {metrics.get('commits_total', 0)}")
            print(f"  Crash Recoveries: {metrics.get('crash_recoveries_total', 0)}")
            print(f"  Replay Entries: {metrics.get('replay_entries_total', 0)}")
            print(f"  Storage Writes: {metrics.get('storage_writes_total', 0)}")
            print(f"  Commands Applied: {metrics.get('commands_applied_total', 0)}")
    else:
        print(f"Error: {result['error_message']}")
        return 1
    
    return 0


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="Raft cluster CLI tool")
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # cluster-info command
    cluster_info_parser = subparsers.add_parser("cluster-info", help="Get cluster information")
    cluster_info_parser.add_argument("--host", default="localhost", help="Server hostname")
    cluster_info_parser.add_argument("--port", type=int, default=50061, help="Server port")
    
    # put-price command
    put_price_parser = subparsers.add_parser("put-price", help="Put a stock price")
    put_price_parser.add_argument("--host", default="localhost", help="Server hostname")
    put_price_parser.add_argument("--port", type=int, default=50061, help="Server port")
    put_price_parser.add_argument("symbol", help="Stock symbol (e.g., AAPL)")
    put_price_parser.add_argument("price", type=float, help="Stock price")
    
    # get-price command
    get_price_parser = subparsers.add_parser("get-price", help="Get a stock price")
    get_price_parser.add_argument("--host", default="localhost", help="Server hostname")
    get_price_parser.add_argument("--port", type=int, default=50061, help="Server port")
    get_price_parser.add_argument("symbol", help="Stock symbol (e.g., AAPL)")
    
    # dump-state command
    dump_state_parser = subparsers.add_parser("dump-state", help="Dump local node state")
    dump_state_parser.add_argument("--host", default="localhost", help="Server hostname")
    dump_state_parser.add_argument("--port", type=int, default=50061, help="Server port")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Run the appropriate command
    if args.command == "cluster-info":
        return asyncio.run(cmd_cluster_info(args))
    elif args.command == "put-price":
        return asyncio.run(cmd_put_price(args))
    elif args.command == "get-price":
        return asyncio.run(cmd_get_price(args))
    elif args.command == "dump-state":
        return asyncio.run(cmd_dump_state(args))
    else:
        print(f"Unknown command: {args.command}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
