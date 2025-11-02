FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build-env
WORKDIR /App
RUN pwd
# Copy everything
COPY . ./
RUN pwd
RUN ls
WORKDIR /App/FKala.Api
RUN pwd
RUN ls
# Restore as distinct layers
RUN dotnet restore
# Build and publish a release
RUN dotnet publish -c Release -o out

# Build runtime image
FROM mcr.microsoft.com/dotnet/aspnet:9.0
WORKDIR /App
COPY --from=build-env /App/FKala.Api/out/ .
EXPOSE 8080/tcp
VOLUME ["/kaladata"]
ENV DataStorage="/kaladata"
ENTRYPOINT ["dotnet", "FKala.Api.dll"]