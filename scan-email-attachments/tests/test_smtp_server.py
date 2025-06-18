"""
Test the SMTP server with mocked DataXray integration.
"""
import pytest
import asyncio
import yaml
import pathlib
from unittest.mock import Mock, patch, AsyncMock, mock_open
from aiosmtpd.smtp import Envelope, Session

from smtp_proxy.smtp_server import EmailSecurityHandler


class TestEmailSecurityHandler:
    """Test the main SMTP handler with DataXray integration."""

    @pytest.fixture
    def blocked_labels_yaml(self):
        """Mock YAML configuration for blocked labels."""
        return {
            "blocked_labels": [
                {
                    "name": "Sensitive Financial Information",
                    "message": "This email contains sensitive financial information that cannot be sent externally",
                }
            ]
        }

    @pytest.fixture
    def handler(self, mock_config, mock_dxr_client, blocked_labels_yaml):
        """Create a handler instance with mocked dependencies."""
        with patch("pathlib.Path.open", mock_open()):
            with patch("yaml.safe_load", return_value=blocked_labels_yaml):
                handler = EmailSecurityHandler()
                handler.forward_message = AsyncMock()
                return handler

    async def test_handle_rcpt_valid_address(self, handler, mock_server, mock_session):
        """Test handling valid RCPT TO command."""
        envelope = Mock()
        envelope.rcpt_tos = []

        result = await handler.handle_RCPT(
            mock_server, mock_session, envelope, "test@example.com", []
        )

        assert result == '250 OK'
        assert "test@example.com" in envelope.rcpt_tos

    async def test_handle_rcpt_invalid_address(self, handler, mock_server, mock_session):
        """Test handling invalid RCPT TO command."""
        envelope = Mock()
        envelope.rcpt_tos = []

        result = await handler.handle_RCPT(
            mock_server, mock_session, envelope, "", []
        )

        assert result == '501 Syntax: RCPT TO: <address>'

    def test_get_sensitive_labels(self, handler, mock_dataxray_hit, mock_dataxray_label):
        """Test extraction of sensitive labels from hits."""
        # Create labels
        sensitive_label = mock_dataxray_label(1, "Sensitive Financial Information")
        safe_label = mock_dataxray_label(
            999, "Safe Content"
        )  # This label name is not blocked

        # Create hits
        hit1 = mock_dataxray_hit("file1.txt", [sensitive_label])
        hit2 = mock_dataxray_hit("file2.txt", [safe_label])
        hit3 = mock_dataxray_hit("file3.txt", [sensitive_label, safe_label])

        hits = [hit1, hit2, hit3]
        restricted_label = handler._get_sensitive_labels(hits)

        # Should return a dictionary with the first blocked label found
        assert restricted_label["name"] == "Sensitive Financial Information"
        assert (
            restricted_label["message"]
            == "This email contains sensitive financial information that cannot be sent externally"
        )

    async def test_handle_data_no_attachments(self, handler, sample_emails, mock_config):
        """Test handling email with no attachments."""
        email = sample_emails("No Attachments")

        envelope = Mock()
        envelope.mail_from = "test@example.com"
        envelope.rcpt_tos = ["recipient@example.com"]
        envelope.content = email.as_bytes()

        result = await handler.handle_DATA(Mock(), Mock(), envelope)

        assert result == '250 Message accepted for delivery'
        handler.forward_message.assert_called_once()

    async def test_handle_data_with_safe_attachment(self, handler, sample_emails, temp_test_files, mock_dxr_client, mock_dataxray_hit, mock_dataxray_label):
        """Test handling email with safe attachment."""
        # Mock DataXray response for safe content
        safe_label = mock_dataxray_label(999, "General Content")
        hit = mock_dataxray_hit("company_report.txt", [safe_label])
        mock_dxr_client.on_demand_classifier.run_job.return_value = [hit]

        email = sample_emails("Safe Attachment", temp_test_files["benign"])

        envelope = Mock()
        envelope.mail_from = "test@example.com"
        envelope.rcpt_tos = ["recipient@example.com"]
        envelope.content = email.as_bytes()

        result = await handler.handle_DATA(Mock(), Mock(), envelope)

        assert result == '250 Message accepted for delivery'
        handler.forward_message.assert_called_once()
        mock_dxr_client.on_demand_classifier.run_job.assert_called_once()

    async def test_handle_data_with_sensitive_attachment(self, handler, sample_emails, temp_test_files, mock_dxr_client, mock_dataxray_hit, mock_dataxray_label):
        """Test handling email with sensitive attachment."""
        # Mock DataXray response for sensitive content
        sensitive_label = mock_dataxray_label(1, "Sensitive Financial Information")  # This label name is in BLACKLISTED_LABEL_NAMES
        hit = mock_dataxray_hit("patient_data.txt", [sensitive_label])
        mock_dxr_client.on_demand_classifier.run_job.return_value = [hit]

        email = sample_emails("Sensitive Attachment", temp_test_files["sensitive"])

        envelope = Mock()
        envelope.mail_from = "test@example.com"
        envelope.rcpt_tos = ["recipient@example.com"]
        envelope.content = email.as_bytes()

        result = await handler.handle_DATA(Mock(), Mock(), envelope)

        assert result.startswith(
            "550 Blocked - This email contains sensitive financial information that cannot be sent externally in patient_data.txt"
        )
        handler.forward_message.assert_not_called()
        mock_dxr_client.on_demand_classifier.run_job.assert_called_once()

    async def test_handle_data_dataxray_error(self, handler, sample_emails, temp_test_files, mock_dxr_client):
        """Test handling DataXray API errors."""
        # Mock DataXray to raise an exception
        mock_dxr_client.on_demand_classifier.run_job.side_effect = Exception("API Error")

        email = sample_emails("Error Test", temp_test_files["benign"])

        envelope = Mock()
        envelope.mail_from = "test@example.com"
        envelope.rcpt_tos = ["recipient@example.com"]
        envelope.content = email.as_bytes()

        result = await handler.handle_DATA(Mock(), Mock(), envelope)

        # Should forward email despite classification error (fail-open behavior)
        assert result == '250 Message accepted for delivery'
        handler.forward_message.assert_called_once()

    async def test_handle_data_invalid_email(self, handler):
        """Test handling invalid email data."""
        envelope = Mock()
        envelope.mail_from = ""  # Invalid sender
        envelope.rcpt_tos = ["recipient@example.com"]
        envelope.content = b"Invalid email"

        result = await handler.handle_DATA(Mock(), Mock(), envelope)

        assert result == '501 Syntax: MAIL FROM: <address>'
        handler.forward_message.assert_not_called()

    def test_blocked_labels_configuration(self, handler, blocked_labels_yaml):
        """Test that blocked labels are properly loaded from configuration."""
        # Check that the handler has the blocked labels loaded
        assert len(handler.blocked_labels) == 1
        assert handler.blocked_labels[0]["name"] == "Sensitive Financial Information"
        assert (
            handler.blocked_labels[0]["message"]
            == "This email contains sensitive financial information that cannot be sent externally"
        )

    async def test_multiple_attachments_mixed_sensitivity(self, handler, temp_test_files, mock_dxr_client, mock_dataxray_hit, mock_dataxray_label):
        """Test email with multiple attachments of mixed sensitivity."""
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.mime.application import MIMEApplication

        # Create email with multiple attachments
        msg = MIMEMultipart()
        msg['From'] = "test@example.com"
        msg['To'] = "recipient@example.com"
        msg['Subject'] = "Mixed Attachments"
        msg.attach(MIMEText("Test body", 'plain'))

        # Add safe attachment
        with open(temp_test_files["benign"], "rb") as f:
            attachment1 = MIMEApplication(f.read())
            attachment1.add_header('Content-Disposition', 'attachment', filename="safe.txt")
            msg.attach(attachment1)

        # Add sensitive attachment
        with open(temp_test_files["sensitive"], "rb") as f:
            attachment2 = MIMEApplication(f.read())
            attachment2.add_header('Content-Disposition', 'attachment', filename="sensitive.txt")
            msg.attach(attachment2)

        # Mock DataXray responses - first call safe, second call sensitive
        safe_label = mock_dataxray_label(999, "General")
        sensitive_label = mock_dataxray_label(1, "Sensitive Financial Information")  # Use label name in BLACKLISTED_LABEL_NAMES

        # Mock multiple calls to run_job
        mock_dxr_client.on_demand_classifier.run_job.side_effect = [
            [mock_dataxray_hit("safe.txt", [safe_label])],  # First attachment - safe
            [mock_dataxray_hit("sensitive.txt", [sensitive_label])]  # Second attachment - sensitive
        ]

        envelope = Mock()
        envelope.mail_from = "test@example.com"
        envelope.rcpt_tos = ["recipient@example.com"]
        envelope.content = msg.as_bytes()

        result = await handler.handle_DATA(Mock(), Mock(), envelope)

        # Should block due to sensitive attachment
        assert result.startswith(
            "550 Blocked - This email contains sensitive financial information that cannot be sent externally in sensitive.txt"
        )
        handler.forward_message.assert_not_called()

        # Should have called DataXray twice but stopped after finding sensitive content
        assert mock_dxr_client.on_demand_classifier.run_job.call_count >= 1
