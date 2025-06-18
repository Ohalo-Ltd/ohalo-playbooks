"""
Test fixtures for email security demo.
"""
import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import Mock, AsyncMock
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

from dxrpy.index.search_results import Hit
from dxrpy.index.labels import Label


@pytest.fixture
def temp_test_files():
    """Create temporary test files for attachment testing."""
    temp_dir = Path(tempfile.mkdtemp())
    
    # Create benign file
    benign_file = temp_dir / "company_report.txt"
    benign_file.write_text("Q4 Company Financial Report\nRevenue: $1M\nProfit: $200K")
    
    # Create sensitive file with patient data
    sensitive_file = temp_dir / "patient_data.txt"
    sensitive_file.write_text("Patient ID: 12345\nName: John Doe\nDate of Birth: 1980-01-01")
    
    # Create financial projections file
    financial_file = temp_dir / "financial_projections.xlsx"
    financial_file.write_bytes(b"PK\x03\x04" + b"fake excel content with financial projections")
    
    yield {
        "benign": benign_file,
        "sensitive": sensitive_file,
        "financial": financial_file,
        "dir": temp_dir
    }
    
    # Cleanup
    import shutil
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_emails():
    """Create sample email messages for testing."""
    
    def create_email(subject="Test Email", attachment_path=None):
        msg = MIMEMultipart()
        msg['From'] = "test@example.com"
        msg['To'] = "recipient@example.com"
        msg['Subject'] = subject
        
        # Add body
        body = "This is a test email for DataXray email security demo."
        msg.attach(MIMEText(body, 'plain'))
        
        # Add attachment if provided
        if attachment_path and Path(attachment_path).exists():
            with open(attachment_path, "rb") as f:
                attachment = MIMEApplication(f.read())
                attachment.add_header(
                    'Content-Disposition', 
                    'attachment', 
                    filename=Path(attachment_path).name
                )
                msg.attach(attachment)
        
        return msg
    
    return create_email


@pytest.fixture
def mock_dataxray_hit():
    """Create mock DataXray Hit objects for testing."""
    
    def create_hit(file_name, labels=None):
        if labels is None:
            labels = []
        
        hit = Mock(spec=Hit)
        hit.file_name = file_name
        hit.labels = labels
        return hit
    
    return create_hit


@pytest.fixture
def mock_dataxray_label():
    """Create mock DataXray Label objects for testing."""
    
    def create_label(label_id, name):
        label = Mock(spec=Label)
        label.id = label_id
        label.name = name
        return label
    
    return create_label


@pytest.fixture
def mock_dxr_client(monkeypatch):
    """Mock the DataXray client."""
    mock_client = Mock()
    mock_classifier = Mock()
    mock_client.on_demand_classifier = mock_classifier
    
    # Mock the constructor
    monkeypatch.setattr('smtp_proxy.smtp_server.DXRClient', lambda *args, **kwargs: mock_client)
    
    return mock_client


@pytest.fixture
def mock_config(monkeypatch):
    """Mock configuration values."""
    mock_cfg = Mock()
    mock_cfg.dataxray_base_url = "https://api.dataxray.com"
    mock_cfg.dataxray_api_key = "test-api-key"
    mock_cfg.datasource_ids = [1, 2, 2788, 2787]
    mock_cfg.smtp_proxy_host = "0.0.0.0"
    mock_cfg.smtp_proxy_port = 1025
    mock_cfg.mailhog_host = "localhost"
    mock_cfg.mailhog_port = 1026
    
    monkeypatch.setattr('smtp_proxy.smtp_server.config', mock_cfg)
    
    return mock_cfg


@pytest.fixture
def handler(mock_config, mock_dxr_client, monkeypatch):
    """Create EmailSecurityHandler with mocked dependencies."""
    from smtp_proxy.smtp_server import EmailSecurityHandler
    
    handler = EmailSecurityHandler()
    
    # Mock the forward_message method since it's now integrated
    handler.forward_message = AsyncMock()
    
    return handler


@pytest.fixture
def mock_envelope():
    """Create a mock SMTP envelope."""
    envelope = Mock()
    envelope.mail_from = "test@example.com"
    envelope.rcpt_tos = ["recipient@example.com"]
    envelope.content = b"Test email content"
    return envelope


@pytest.fixture
def mock_session():
    """Create a mock SMTP session."""
    return Mock()


@pytest.fixture
def mock_server():
    """Create a mock SMTP server."""
    return Mock()
