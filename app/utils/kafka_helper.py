import os

def get_kafka_ssl_config():
    """Genera certificados temporales y devuelve la config SSL para Aiven"""
    cert_dir = "/tmp/kafka_certs"
    os.makedirs(cert_dir, exist_ok=True)
    
    ca_path = os.path.join(cert_dir, "ca.pem")
    cert_path = os.path.join(cert_dir, "service.cert")
    key_path = os.path.join(cert_dir, "service.key")

    # Escribir contenido de variables de entorno a archivos
    with open(ca_path, "w") as f: f.write(os.getenv("KAFKA_CA_CERT", ""))
    with open(cert_path, "w") as f: f.write(os.getenv("KAFKA_ACCESS_CERT", ""))
    with open(key_path, "w") as f: f.write(os.getenv("KAFKA_ACCESS_KEY", ""))

    return {
        'security.protocol': 'SSL',
        'ssl.ca.location': ca_path,
        'ssl.certificate.location': cert_path,
        'ssl.key.location': key_path,
        'ssl.endpoint.identification.algorithm': 'https',
    }