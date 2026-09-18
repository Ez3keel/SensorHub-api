using SensorHub.Infrastructure;

var builder = Host.CreateApplicationBuilder(args);

// Ordem importa: hosted services iniciam na ordem do registro (tópicos e migrations antes do consumer).
builder.Services.AddKafkaMessaging(builder.Configuration);
builder.Services.AddPostgresPersistence(builder.Configuration);
builder.Services.AddReadingPersistenceConsumer(builder.Configuration);

builder.Build().Run();
