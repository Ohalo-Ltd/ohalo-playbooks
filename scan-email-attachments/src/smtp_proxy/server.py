#!/usr/bin/env python3
"""
SMTP Proxy Server Entry Point

This module starts the SMTP proxy server that intercepts emails,
classifies attachments using DataXray, and blocks sensitive content.
"""

import asyncio
import logging
import sys
import signal

from .config import config
from .smtp_server import SMTPProxyServer

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.log_level.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def main():
    """Main entry point for the SMTP proxy server."""
    
    # Validate configuration
    config_error = config.validate()
    if config_error:
        logger.error(f"Configuration error: {config_error}")
        sys.exit(1)
    
    logger.info("Starting DataXray Email Security Demo")
    logger.info(f"DataXray Base URL: {config.dataxray_base_url}")
    logger.info(f"DataXray Datasource IDs: {config.datasource_ids}")
    logger.info(f"SMTP Proxy: {config.smtp_proxy_host}:{config.smtp_proxy_port}")
    logger.info(f"MailHog Relay: {config.mailhog_host}:{config.mailhog_port}")
    
    # Create and start the SMTP proxy server
    proxy_server = SMTPProxyServer()
    
    def signal_handler(signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, shutting down...")
        proxy_server.stop()
        sys.exit(0)
    
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Start the server
        proxy_server.start()
        
        logger.info("SMTP proxy is ready to accept connections")
        logger.info("Press Ctrl+C to stop the server")
        
        # Keep the server running
        try:
            while True:
                asyncio.get_event_loop().run_until_complete(asyncio.sleep(1))
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
            
    except Exception as e:
        logger.error(f"Failed to start SMTP proxy: {e}")
        sys.exit(1)
    finally:
        proxy_server.stop()
        logger.info("SMTP proxy server stopped")


if __name__ == "__main__":
    main()
