from google.cloud import secretmanager
import json

class SecretManager:
    @staticmethod
    def get_aws_credentials(project_id, secret_id):
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(name=name)
        payload = response.payload.data.decode("UTF-8").strip()  # Strip whitespace
        
        try:
            credentials = json.loads(payload)
            # Validate and clean the credentials
            access_key = credentials['access_key_id'].strip()
            secret_key = credentials['secret_access_key'].strip()
            
            # Validate that credentials don't contain invalid characters
            if not access_key or not secret_key:
                raise ValueError("Empty credentials found")
                
            return access_key, secret_key
        except (json.JSONDecodeError, KeyError) as e:
            raise ValueError(f"Invalid credentials format in secret: {e}")
