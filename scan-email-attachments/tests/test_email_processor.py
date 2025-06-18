"""
Test the EmailProcessor class.
"""
import pytest
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

from smtp_proxy.email_processor import EmailProcessor


class TestEmailProcessor:
    """Test email processing functionality."""
    
    def test_parse_message_from_bytes(self):
        """Test parsing email from bytes."""
        email_content = b"""From: test@example.com
To: recipient@example.com
Subject: Test Email

This is a test email body."""
        
        processor = EmailProcessor()
        message = processor.parse_message(email_content)
        
        assert message['From'] == 'test@example.com'
        assert message['To'] == 'recipient@example.com'
        assert message['Subject'] == 'Test Email'
    
    def test_parse_message_from_string(self):
        """Test parsing email from string."""
        email_content = """From: test@example.com
To: recipient@example.com
Subject: Test Email

This is a test email body."""
        
        processor = EmailProcessor()
        message = processor.parse_message(email_content)
        
        assert message['From'] == 'test@example.com'
        assert message['To'] == 'recipient@example.com'
        assert message['Subject'] == 'Test Email'
    
    def test_parse_message_invalid_input(self):
        """Test error handling for invalid message data."""
        processor = EmailProcessor()
        
        with pytest.raises(ValueError, match="Message data cannot be None"):
            processor.parse_message(None)
    
    def test_extract_attachments_no_attachments(self, sample_emails):
        """Test extracting attachments from email with no attachments."""
        email = sample_emails("No Attachment Test")
        processor = EmailProcessor()
        
        attachments = processor.extract_attachments(email)
        assert len(attachments) == 0
    
    def test_extract_attachments_with_attachment(self, sample_emails, temp_test_files):
        """Test extracting attachments from email with attachment."""
        email = sample_emails("With Attachment", temp_test_files["benign"])
        processor = EmailProcessor()
        
        attachments = processor.extract_attachments(email)
        assert len(attachments) == 1
        
        filename, content = attachments[0]
        assert filename == "company_report.txt"
        assert b"Company Financial Report" in content
    
    def test_get_message_info(self, sample_emails):
        """Test getting message information."""
        email = sample_emails("Test Subject")
        processor = EmailProcessor()
        
        info = processor.get_message_info(email)
        
        assert info['subject'] == 'Test Subject'
        assert info['from'] == 'test@example.com'
        assert info['to'] == 'recipient@example.com'
        assert 'date' in info
    
    def test_extract_multiple_attachments(self, temp_test_files):
        """Test extracting multiple attachments."""
        # Create email with multiple attachments
        msg = MIMEMultipart()
        msg['From'] = "test@example.com"
        msg['To'] = "recipient@example.com"
        msg['Subject'] = "Multiple Attachments"
        
        # Add body
        msg.attach(MIMEText("Test email body", 'plain'))
        
        # Add first attachment
        with open(temp_test_files["benign"], "rb") as f:
            attachment1 = MIMEApplication(f.read())
            attachment1.add_header(
                'Content-Disposition', 
                'attachment', 
                filename="report1.txt"
            )
            msg.attach(attachment1)
        
        # Add second attachment
        with open(temp_test_files["sensitive"], "rb") as f:
            attachment2 = MIMEApplication(f.read())
            attachment2.add_header(
                'Content-Disposition', 
                'attachment', 
                filename="data.txt"
            )
            msg.attach(attachment2)
        
        processor = EmailProcessor()
        attachments = processor.extract_attachments(msg)
        
        assert len(attachments) == 2
        assert attachments[0][0] == "report1.txt"
        assert attachments[1][0] == "data.txt"
