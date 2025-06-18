import email
import logging
from email.message import EmailMessage, Message
from typing import List, Tuple, Dict, Any, Union

logger = logging.getLogger(__name__)


class EmailProcessor:
    """Process and extract attachments from email messages."""
    
    @staticmethod
    def parse_message(message_data) -> Union[EmailMessage, Message]:
        """Parse raw email message data into EmailMessage object."""
        try:
            if isinstance(message_data, bytes):
                return email.message_from_bytes(message_data)
            elif isinstance(message_data, str):
                return email.message_from_string(message_data)
            elif message_data is None:
                raise ValueError("Message data cannot be None")
            else:
                # Try to convert to bytes if possible
                return email.message_from_bytes(bytes(message_data))
        except Exception as e:
            logger.error(f"Failed to parse email message: {e}")
            raise ValueError(f"Invalid email format: {e}")
    
    @staticmethod
    def extract_attachments(message: Union[EmailMessage, Message]) -> List[Tuple[str, bytes]]:
        """
        Extract attachments from email message.
        
        Args:
            message: Parsed email message
            
        Returns:
            List of tuples (filename, content_bytes)
        """
        attachments = []
        
        if message.is_multipart():
            for part in message.walk():
                # Skip multipart containers and text parts
                if part.get_content_maintype() == 'multipart':
                    continue
                    
                # Check if this part is an attachment
                content_disposition = part.get("Content-Disposition", "")
                if "attachment" in content_disposition:
                    filename = part.get_filename()
                    if filename:
                        try:
                            content = part.get_payload(decode=True)
                            if content:
                                attachments.append((filename, content))
                                logger.info(f"Extracted attachment: {filename} ({len(content)} bytes)")
                        except Exception as e:
                            logger.warning(f"Failed to extract attachment {filename}: {e}")
        
        return attachments
    
    @staticmethod
    def get_message_info(message: Union[EmailMessage, Message]) -> Dict[str, Any]:
        """Extract basic message information."""
        return {
            "from": message.get("From", ""),
            "to": message.get("To", ""),
            "subject": message.get("Subject", ""),
            "message_id": message.get("Message-ID", ""),
            "date": message.get("Date", "")
        }
    
    @staticmethod
    def message_to_bytes(message: Union[EmailMessage, Message]) -> bytes:
        """Convert EmailMessage back to bytes for forwarding."""
        if hasattr(message, 'as_bytes'):
            return message.as_bytes()
        return message.as_string().encode('utf-8')
