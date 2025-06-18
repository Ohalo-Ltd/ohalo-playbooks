"""
Integration tests for the complete email security workflow.
"""
import pytest
import asyncio
import smtplib
import time
from unittest.mock import Mock, patch, mock_open, AsyncMock
from pathlib import Path
import yaml

from smtp_proxy.smtp_server import EmailSecurityHandler


class TestEmailSecurityIntegration:
    """Integration tests for complete email security workflow."""

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

    @pytest.mark.asyncio
    async def test_complete_workflow_safe_email(self, handler, sample_emails, temp_test_files, mock_dxr_client, mock_dataxray_hit, mock_dataxray_label):
        """Test complete workflow with safe email."""
        # Mock safe DataXray response
        safe_label = mock_dataxray_label(999, "General Content")
        hit = mock_dataxray_hit("company_report.txt", [safe_label])
        mock_dxr_client.on_demand_classifier.run_job.return_value = [hit]

        # Create test email
        email = sample_emails("Integration Test - Safe", temp_test_files["benign"])

        envelope = Mock()
        envelope.mail_from = "test@example.com"
        envelope.rcpt_tos = ["recipient@example.com"]
        envelope.content = email.as_bytes()

        # Process email
        result = await handler.handle_DATA(Mock(), Mock(), envelope)

        # Verify successful delivery
        assert result == '250 Message accepted for delivery'
        handler.forward_message.assert_called_once_with(
            "test@example.com",
            ["recipient@example.com"],
            email.as_bytes()
        )

    @pytest.mark.asyncio
    async def test_complete_workflow_blocked_email(self, handler, sample_emails, temp_test_files, mock_dxr_client, mock_dataxray_hit, mock_dataxray_label):
        """Test complete workflow with blocked email."""
        # Mock sensitive DataXray response
        sensitive_label = mock_dataxray_label(1, "Sensitive Financial Information")  # Use label name in BLACKLISTED_LABEL_NAMES
        hit = mock_dataxray_hit("patient_data.txt", [sensitive_label])
        mock_dxr_client.on_demand_classifier.run_job.return_value = [hit]

        # Create test email
        email = sample_emails("Integration Test - Blocked", temp_test_files["sensitive"])

        envelope = Mock()
        envelope.mail_from = "test@example.com"
        envelope.rcpt_tos = ["recipient@example.com"]
        envelope.content = email.as_bytes()

        # Process email
        result = await handler.handle_DATA(Mock(), Mock(), envelope)

        # Verify email was blocked
        assert result.startswith(
            "550 Blocked - This email contains sensitive financial information that cannot be sent externally in patient_data.txt"
        )
        handler.forward_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_error_handling_and_recovery(self, handler, sample_emails, temp_test_files, mock_dxr_client):
        """Test error handling and fail-open behavior."""
        # Mock DataXray to fail
        mock_dxr_client.on_demand_classifier.run_job.side_effect = Exception("API temporarily unavailable")

        email = sample_emails("Error Recovery Test", temp_test_files["benign"])

        envelope = Mock()
        envelope.mail_from = "test@example.com"
        envelope.rcpt_tos = ["recipient@example.com"]
        envelope.content = email.as_bytes()

        # Process email
        result = await handler.handle_DATA(Mock(), Mock(), envelope)

        # Should fail-open (deliver email despite error)
        assert result == '250 Message accepted for delivery'
        handler.forward_message.assert_called_once()

    def test_sensitive_label_coverage(self, handler, blocked_labels_yaml):
        """Test that all required sensitive labels are covered from YAML config."""
        # Verify expected sensitive labels are configured
        expected_label = "Sensitive Financial Information"

        # Check if the expected label is in the handler's blocked labels
        found = any(label["name"] == expected_label for label in handler.blocked_labels)
        assert found, f"Expected label '{expected_label}' not found in blocked labels"

    @pytest.mark.asyncio
    async def test_performance_timing(self, handler, sample_emails, temp_test_files, mock_dxr_client, mock_dataxray_hit, mock_dataxray_label):
        """Test that email processing completes within reasonable time."""
        # Mock DataXray with delay to simulate real API
        def slow_classification(*args, **kwargs):
            import time
            time.sleep(0.1)  # Simulate 100ms API call
            safe_label = mock_dataxray_label(999, "General")
            return [mock_dataxray_hit("test.txt", [safe_label])]

        mock_dxr_client.on_demand_classifier.run_job.side_effect = slow_classification

        email = sample_emails("Performance Test", temp_test_files["benign"])

        envelope = Mock()
        envelope.mail_from = "test@example.com"
        envelope.rcpt_tos = ["recipient@example.com"]
        envelope.content = email.as_bytes()

        # Measure processing time
        start_time = time.time()
        result = await handler.handle_DATA(Mock(), Mock(), envelope)
        end_time = time.time()

        # Should complete within reasonable time (allowing for API delay)
        processing_time = end_time - start_time
        assert processing_time < 5.0  # Should complete within 5 seconds
        assert result == '250 Message accepted for delivery'
