# Thunderbird Configuration Guide

## Quick Setup Instructions

### 1. Open Account Settings
- Open Thunderbird
- Go to **Tools → Account Settings** (or **Edit → Account Settings** on macOS)

### 2. Configure Outgoing Server (SMTP)
- In the left panel, click **Outgoing Server (SMTP)**
- Click **Add...** to create a new SMTP server

### 3. SMTP Server Settings
Configure the new server with these exact settings:

```
Server Name: localhost
Port: 1025
Connection Security: None
Authentication Method: No authentication
User Name: (leave empty)
```

### 4. Set as Default
- After creating the server, select it in the list
- Click **Set Default** to make it your default outgoing server

### 5. Test Configuration
- Create a new email
- Try sending with different attachments

## Troubleshooting

### "Connection Refused" Error
- Make sure Docker services are running: `docker-compose up`
- Check if port 1025 is available: `lsof -i :1025`
- Verify firewall settings

### "Authentication Failed" Error
- Ensure **Authentication Method** is set to **No authentication**
- Verify **User Name** field is empty

### "Certificate" Warnings
- Set **Connection Security** to **None**
- Thunderbird may still warn about unencrypted connections - this is expected for the demo

### Still Having Issues?
1. Restart Thunderbird completely
2. Check Docker logs: `docker-compose logs smtp-proxy`
3. Try the test script: `python test_smtp.py`

## Alternative Mail Clients

### Apple Mail (macOS)
1. Mail → Preferences → Accounts
2. Select your account → Server Settings
3. Outgoing Mail Server (SMTP): localhost:1025
4. Authentication: None

### Outlook
1. File → Account Settings → Account Settings
2. Select account → Change
3. More Settings → Advanced
4. Outgoing server (SMTP): localhost, Port: 1025
5. Uncheck "Requires authentication"

### Gmail/Web Clients
Web-based email clients cannot be configured to use localhost SMTP servers.
Use Thunderbird or another desktop email client for testing.
