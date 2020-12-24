# ADLS storage location folder
from utils.connection import get_secret

mount_point = "/mnt/processing-rcb-working/"

# Wenco database constants
# TODO: Remove the user data from the constants
# TODO: Find a better way to provide credentials
wenco_user = "svc.azrAIPipeline@teck.com"
wenco_secret = get_secret("teck-landing-sqlmi-secret", "application-shared")
wenco_hostname = "sqlmi-shared-data-prod-centralus-001.public.f02d6179b9d9.database.windows.net"
wenco_database = "sqldb-wencoevo-prod"
shadow_database = "sqldb-shadowdsvc-prod"
wenco_port = 3342


def wenco_properties():
    connection_properties = {
        "user": wenco_user,
        "password": wenco_secret,
        "encrypt": "true",
        "Authentication": "ActiveDirectoryPassword",
        "trustServerCertificate": "true",
        "hostNameInCertificate": "*.database.windows.net"
    }
    return connection_properties


# RCB database constants
# TODO: Remove the user data from the constants
# TODO: Find a better way to provide credentials
rcb_user = get_secret("rcb-sql-database-user", "processing-rcb-aa-scope")
rcb_secret = get_secret("rcb-sql-database-secret", "processing-rcb-aa-scope")
rcb_hostname = get_secret("rcb-sql-server-host", "processing-rcb-aa-scope")
rcb_database = get_secret("rcb-sql-database", "processing-rcb-aa-scope")
rcb_port = 1433


def rcb_properties():
    connection_properties = {
        "user": rcb_user,
        "password": rcb_secret
    }
    return connection_properties
