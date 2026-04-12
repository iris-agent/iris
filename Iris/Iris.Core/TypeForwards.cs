using System.Runtime.CompilerServices;
using Iris.Core;
using Iris.Core.Plugins;

[assembly: TypeForwardedTo(typeof(DataMessage))]
[assembly: TypeForwardedTo(typeof(ITransport))]
[assembly: TypeForwardedTo(typeof(IConnector))]
[assembly: TypeForwardedTo(typeof(IPluginMetadata))]
[assembly: TypeForwardedTo(typeof(PluginAttribute))]
[assembly: TypeForwardedTo(typeof(PluginType))]
[assembly: TypeForwardedTo(typeof(IPluginFactory))]
[assembly: TypeForwardedTo(typeof(IPluginRegistry))]
[assembly: TypeForwardedTo(typeof(IPluginRegistrar))]
[assembly: TypeForwardedTo(typeof(IPluginActivator))]
