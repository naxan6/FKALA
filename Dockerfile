FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build-env
WORKDIR /App
COPY . ./
WORKDIR /App/FKala.Api
RUN dotnet restore
RUN dotnet publish -c Release -o out

# Build runtime image
FROM mcr.microsoft.com/dotnet/aspnet:9.0
RUN adduser --disabled-password --no-create-home appuser
WORKDIR /App
COPY --from=build-env /App/FKala.Api/out/ .
EXPOSE 8080/tcp
VOLUME ["/kaladata"]
ENV DataStorage="/kaladata"
USER appuser
ENTRYPOINT ["dotnet", "FKala.Api.dll"]