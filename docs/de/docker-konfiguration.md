# Docker-Konfiguration für FKALA

## Docker-Build-Prozess

### Basis-Image und Build-Kommando

Das FKALA-System kann einfach als Docker-Container gebaut werden:

```bash
docker build -t fkala:latest .
```

### Dockerfile

Das Dockerfile für FKALA ist wie folgt aufgebaut:

```dockerfile
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
WORKDIR /app
EXPOSE 8080

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY ["FKala.Api/FKala.Api.csproj", "FKala.Api/"]
COPY ["FKala.Core/FKala.Core.csproj", "FKala.Core/"]
COPY ["FKala.Core.Interfaces/FKala.Core.Interfaces.csproj", "FKala.Core.Interfaces/"]
COPY ["FKala.Background/FKala.Background.csproj", "FKala.Background/"]
COPY ["FKala.Client.Console/FKala.Client.Console.csproj", "FKala.Client.Console/"]
COPY ["FKala.WebApi/FKala.WebApi.csproj", "FKala.WebApi/"]

RUN dotnet restore "./FKala.Api/FKala.Api.csproj"
COPY . .
WORKDIR "/src/FKala.Api"
RUN dotnet build "FKala.Api.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "FKala.Api.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "FKala.Api.dll"]
```

## Docker-Container-Start

### Grundlegender Container-Start

```bash
docker run -d -p 8080:8080 --name fkala-container fkala:latest
```

### Container mit benutzerdefinierter Konfiguration

```bash
docker run -d \
  --name fkala-container \
  -p 8080:8080 \
  -v /path/to/local/data:/data \
  -e "DataStorage=/data" \
  fkala:latest
```

## Docker-Netzwerkkonfiguration

### Benutzerdefiniertes Netzwerk erstellen

```bash
docker network create fkala-network
```

### Container im benutzerdefinierten Netzwerk starten

```bash
docker run -d \
  --name fkala-container \
  --network fkala-network \
  -p 8080:8080 \
  fkala:latest
```

## Umgebungsvariablen

Das FKALA-System unterstützt folgende Umgebungsvariablen:

### Datenverwaltung

**DataStorage**
- **Beschreibung:** Basispfad für die Datenspeicherung
- **Standard:** `/data`
- **Beispiel:** `DATA_STORAGE=/opt/fkala/data`

### Performance

**ReadBuffer**
- **Beschreibung:** Größe des Lesebuffers in Bytes
- **Standard:** `16384` (16 KB)

**WriteBuffer**
- **Beschreibung:** Größe des Schreibbuffers in Bytes
- **Standard:** `32768` (32 KB)

### Logging

**DOTNET_LOGGING__LOGLEVEL__DEFAULT**
- **Beschreibung:** Allgemeines Log-Level
- **Möglichkeiten:** `Debug`, `Information`, `Warning`, `Error`, `Critical`
- **Standard:** `Information`

## Docker Compose Integration

Für eine einfache lokale Entwicklung kann auch Docker Compose verwendet werden:

```yaml
version: '3.8'
services:
  fkala:
    build: .
    ports:
      - "8080:8080"
    volumes:
      - ./data:/data
    environment:
      - DataStorage=/data
      - ReadBuffer=16384
      - WriteBuffer=32768
```

## Container-Management

### Container auflisten

```bash
docker ps
```

### Container Logs anzeigen

```bash
docker logs fkala-container
```

### Container stoppen und entfernen

```bash
docker stop fkala-container
docker rm fkala-container
```

## Docker-Performance-Tuning

### Buffer-Konfiguration für große Datenmengen

```bash
docker run -d \
  --name fkala-container \
  -p 8080:8080 \
  -e "ReadBuffer=32768" \
  -e "WriteBuffer=65536" \
  fkala:latest
```

## Sicherheitsaspekte

### Container mit eingeschränkten Rechten

```bash
docker run -d \
  --name fkala-container \
  --read-only \
  --tmpfs /tmp \
  -v /data:/data:ro \
  fkala:latest
```

## Docker-Optimierung

### Multi-Stage Build für kleinere Images

Das Dockerfile verwendet einen Multi-Stage Build, um ein möglichst kleines Endimage zu erstellen.

### Image-Größe optimieren

```bash
docker system prune -a
docker image prune -a
