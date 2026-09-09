"""Generate private, disposable lab certificates without publishing key material."""

from pathlib import Path
import os
import subprocess


def generate_tls(directory, namespace, password, *, openssl="openssl", keytool="keytool"):
    directory = Path(directory)
    directory.mkdir(mode=0o700)
    password_file = directory / "store-password"
    password_file.write_text(password)
    os.chmod(password_file, 0o600)

    def run(*command):
        subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

    run(openssl, "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-sha256", "-days", "7",
        "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign",
        "-subj", "/CN=Disposable transaction lab CA", "-keyout", str(directory / "ca.key"), "-out", str(directory / "ca.crt"))
    for name in ["gateway", "trino-blue", "trino-green"]:
        key = directory / (name + ".key")
        csr = directory / (name + ".csr")
        certificate = directory / (name + ".crt")
        extensions = directory / (name + ".ext")
        extensions.write_text("basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n"
                              f"subjectAltName=DNS:{name},DNS:{name}.{namespace}.svc,DNS:{name}.{namespace}.svc.cluster.local,DNS:localhost,IP:127.0.0.1\n")
        run(openssl, "req", "-new", "-newkey", "rsa:2048", "-nodes", "-sha256", "-subj", "/CN=" + name,
            "-keyout", str(key), "-out", str(csr))
        run(openssl, "x509", "-req", "-in", str(csr), "-CA", str(directory / "ca.crt"), "-CAkey", str(directory / "ca.key"),
            "-CAcreateserial", "-days", "7", "-sha256", "-extfile", str(extensions), "-out", str(certificate))
        run(openssl, "pkcs12", "-export", "-name", name, "-inkey", str(key), "-in", str(certificate),
            "-certfile", str(directory / "ca.crt"), "-out", str(directory / (name + ".p12")), "-passout", "file:" + str(password_file))
    run(keytool, "-importcert", "-noprompt", "-alias", "transaction-lab-ca", "-file", str(directory / "ca.crt"),
        "-keystore", str(directory / "truststore.p12"), "-storetype", "PKCS12", "-storepass:file", str(password_file))
    for path in directory.iterdir():
        os.chmod(path, 0o600)
