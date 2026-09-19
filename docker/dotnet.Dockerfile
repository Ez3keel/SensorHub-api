# Imagem única para os hosts .NET (API, Worker, Simulador): docker build --build-arg PROJECT=src/SensorHub.Api ...
# Multi-stage: o SDK compila; a imagem final leva só o runtime e roda SEM root (usuário "app" da imagem base).
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG PROJECT
WORKDIR /repo

# Restaura primeiro só com os .csproj: a camada de pacotes NuGet só é refeita quando as dependências mudam.
COPY Directory.Build.props ./
COPY src/SensorHub.Domain/*.csproj src/SensorHub.Domain/
COPY src/SensorHub.Application/*.csproj src/SensorHub.Application/
COPY src/SensorHub.Infrastructure/*.csproj src/SensorHub.Infrastructure/
COPY src/SensorHub.Api/*.csproj src/SensorHub.Api/
COPY src/SensorHub.Worker/*.csproj src/SensorHub.Worker/
COPY src/SensorHub.MqttBridge/*.csproj src/SensorHub.MqttBridge/
COPY tools/SensorHub.Simulator/*.csproj tools/SensorHub.Simulator/
RUN dotnet restore ${PROJECT}

COPY src/ src/
COPY tools/ tools/
RUN dotnet publish ${PROJECT} -c Release --no-restore -o /out /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=build /out .
USER $APP_UID
ARG ENTRY
ENV ENTRY_DLL=${ENTRY}
ENTRYPOINT ["sh", "-c", "exec dotnet ${ENTRY_DLL} \"$@\"", "--"]
