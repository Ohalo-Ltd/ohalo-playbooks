import logging
import os
import tempfile
import asyncio
import functools
import smtplib
import yaml
import pathlib
from aiosmtpd.controller import Controller
from aiosmtpd.smtp import SMTP as SMTPProtocol, Envelope
from aiosmtpd.smtp import Session
from typing import List, Dict, Union, Any, Optional

from dxrpy import DXRClient
from dxrpy.utils import File
from dxrpy.index.search_results import Hit

from .config import config
from .email_processor import EmailProcessor

logger = logging.getLogger(__name__)

class EmailSecurityHandler:
    """SMTP handler that intercepts emails and classifies attachments."""

    def __init__(self):
        self.email_processor = EmailProcessor()
        # Initialize the DataXray client directly
        self.dxr_client = DXRClient(
            config.dataxray_base_url, config.dataxray_api_key, ignore_ssl=True
        )
        # Mail relay configuration
        self.mailhog_host = config.mailhog_host
        self.mailhog_port = config.mailhog_port

        # Load blocked labels configuration
        self.blocked_labels = self._load_blocked_labels()

    def _load_blocked_labels(self) -> List[Dict[str, str]]:
        """Load blocked labels from YAML configuration file."""
        try:
            # Get the path to the blocked_labels.yaml file (in same directory as this file)
            yaml_path = pathlib.Path(__file__).parent / "blocked_labels.yaml"

            with open(yaml_path, "r") as file:
                config_data = yaml.safe_load(file)

            if not config_data or "blocked_labels" not in config_data:
                logger.warning("No blocked labels found in configuration file")
                return []

            logger.info(
                f"Loaded {len(config_data['blocked_labels'])} blocked labels from configuration"
            )
            return config_data["blocked_labels"]
        except Exception as e:
            logger.error(f"Error loading blocked labels configuration: {e}")
            return []

    async def forward_message(
        self, sender: str, recipients: List[str], message_data: Union[bytes, str]
    ) -> None:
        """
        Forward email message to MailHog.

        Args:
            sender: Email sender address
            recipients: List of recipient addresses
            message_data: Raw email message data (bytes or str)
        """
        try:
            logger.info(f"Forwarding email from {sender} to {recipients}")

            # Use synchronous SMTP client (aiosmtpd doesn't have async relay built-in)
            with smtplib.SMTP(self.mailhog_host, self.mailhog_port) as smtp:
                # MailHog doesn't require authentication
                # Convert message data to bytes if it's a string
                if isinstance(message_data, str):
                    message_data = message_data.encode("utf-8")
                smtp.sendmail(sender, recipients, message_data)

            logger.info("Email successfully forwarded to MailHog")

        except smtplib.SMTPException as e:
            logger.error(f"SMTP error while forwarding email: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error while forwarding email: {e}")
            raise

    async def handle_RCPT(self, server: SMTPProtocol, session: Session, envelope: Envelope, 
                         address: str, rcpt_options: List[str]):
        """Handle RCPT TO command - accept all recipients for demo."""
        if not address:
            return '501 Syntax: RCPT TO: <address>'
        envelope.rcpt_tos.append(address)
        return '250 OK'

    def _get_sensitive_labels(self, hits: List[Hit]) -> Dict[str, str]:
        """
        Extract sensitive labels from classification hits.

        Args:
            hits: List of Hit objects from DataXray classification

        Returns:
            Dictionary with name and message for the first blocked label that was found
        """
        # Create a set of blocked label names for efficient lookup
        blocked_names = {item["name"]: item["message"] for item in self.blocked_labels}

        for hit in hits:
            logger.info(
                f"Processing hit for file: {hit.file_name} with labels: {[label.name for label in hit.labels]}"
            )
            for label in hit.labels:
                if label.name in blocked_names:
                    # Return the first blocked label and its message
                    return {"name": label.name, "message": blocked_names[label.name]}

        # No blocked labels found
        return {}

    async def handle_DATA(self, server: SMTPProtocol, session: Session, envelope: Envelope):
        """
        Handle email data - main classification logic.
        
        This is where we:
        1. Parse the email and extract attachments
        2. Classify each attachment with DataXray
        3. Block if highly sensitive, otherwise relay to MailHog
        """
        try:
            # Parse the email message
            message = self.email_processor.parse_message(envelope.content)
            message_info = self.email_processor.get_message_info(message)

            logger.info(f"Processing email: {message_info['subject']} from {envelope.mail_from}")

            if not envelope.mail_from or not envelope.content:
                return '501 Syntax: MAIL FROM: <address>'

            # Extract attachments
            attachments = self.email_processor.extract_attachments(message)

            if not attachments:
                # No attachments - safe to forward
                logger.info("No attachments found, forwarding email")
                await self.forward_message(
                    envelope.mail_from, envelope.rcpt_tos, envelope.content
                )
                return '250 Message accepted for delivery'

            # Classify each attachment
            for filename, content in attachments:
                try:
                    # Create a temporary file with the content
                    temp_file_path = None
                    try:
                        # Create temporary file with proper extension if possible
                        file_extension = os.path.splitext(filename)[1] if '.' in filename else ''

                        with tempfile.NamedTemporaryFile(
                            delete=False, 
                            suffix=file_extension,
                            prefix="email_attachment_"
                        ) as temp_file:
                            temp_file.write(content)
                            temp_file_path = temp_file.name

                        logger.info(f"Classifying attachment: {filename} ({len(content)} bytes)")

                        # Create File object for DataXray
                        files = [File(temp_file_path)]

                        # Run the synchronous Data X-Ray call in a thread pool
                        loop = asyncio.get_event_loop()
                        hits = await loop.run_in_executor(
                            None,
                            functools.partial(
                                self.dxr_client.on_demand_classifier.run_job,
                                files,
                                config.datasource_ids
                            )
                        )

                        # Check for sensitive content
                        restricted_label = self._get_sensitive_labels(hits)

                        if restricted_label:
                            logger.warning(f"Blocking email with sensitive attachment: {filename}")
                            return f"550 Blocked - {restricted_label['message']} in {filename}"

                    finally:
                        # Clean up temporary file
                        if temp_file_path and os.path.exists(temp_file_path):
                            try:
                                os.unlink(temp_file_path)
                            except Exception as e:
                                logger.warning(f"Failed to delete temporary file {temp_file_path}: {e}")

                except Exception as e:
                    logger.warning(
                        f"Classification error - allowing email through for demo: {str(e)}"
                    )

            # All attachments passed classification - forward to MailHog
            await self.forward_message(
                envelope.mail_from, envelope.rcpt_tos, envelope.content
            )
            return '250 Message accepted for delivery'

        except Exception as e:
            logger.error(f"Error processing email: {e}")
            return '451 Internal server error - please try again later'


class SMTPProxyServer:
    """SMTP proxy server using aiosmtpd."""
    
    def __init__(self):
        self.handler = EmailSecurityHandler()
        self.controller = None
    
    def start(self):
        """Start the SMTP proxy server."""
        logger.info(f"Starting SMTP proxy on {config.smtp_proxy_host}:{config.smtp_proxy_port}")
        
        self.controller = Controller(
            self.handler,
            hostname=config.smtp_proxy_host,
            port=config.smtp_proxy_port
        )
        self.controller.start()
        
        logger.info("SMTP proxy server started successfully")
        return self.controller
    
    def stop(self):
        """Stop the SMTP proxy server."""
        if self.controller:
            self.controller.stop()
            logger.info("SMTP proxy server stopped")
