// See https://aka.ms/new-console-template for more information


using BenchmarkDotNet.Running;

//await TopicInitializer.Initialize();

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);