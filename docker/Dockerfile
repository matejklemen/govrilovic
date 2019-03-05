FROM postgres:alpine
# Copies the SQL script so it runs after initialization. Only the first time.
COPY baza.sql /docker-entrypoint-initdb.d/
