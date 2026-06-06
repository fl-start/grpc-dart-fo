@TestOn('vm')
library;

import 'dart:convert';
import 'dart:io';

import 'package:grpc/grpc.dart';
import 'package:grpc/src/client/failover/config.dart';
import 'package:grpc/src/client/failover/context.dart';
import 'package:grpc/src/client/failover/failover_channel.dart';
import 'package:grpc/src/server/server.dart';
import 'package:grpc/src/server/service.dart';
import 'package:test/test.dart';

class _EchoService extends Service {
  @override
  String get $name => 'test.Echo';

  _EchoService() {
    $addMethod(
      ServiceMethod<String, String>(
        'Say',
        _say,
        false,
        false,
        (List<int> value) => utf8.decode(value),
        (String value) => utf8.encode(value),
      ),
    );
  }

  Future<String> _say(ServiceCall call, Future<String> request) async =>
      'pong:${await request}';
}

void main() {
  group('FailoverClientChannel integration', () {
    late Server server;
    late int port;
    late String loopback;

    setUp(() async {
      loopback = InternetAddress.loopbackIPv4.address;
      server = Server.create(services: [_EchoService()]);
      await server.serve(
        address: InternetAddress.loopbackIPv4,
        port: 0,
      );
      port = server.port!;
    });

    tearDown(() async {
      await server.shutdown();
    });

    test('unary RPC over failover channel', () async {
      final ctx = GrpcFoContext(
        const GrpcFoConfig(
          connectTimeout: Duration(seconds: 3),
          tcpRace: false,
        ),
        lookup: (_) async => [InternetAddress(loopback)],
      );

      final channel = FailoverClientChannel(
        'grpc-fo.test',
        port: port,
        foContext: ctx,
        options: const ChannelOptions(
          credentials: ChannelCredentials.insecure(),
        ),
      );
      addTearDown(channel.shutdown);

      final client = Client(
        channel,
        options: CallOptions(timeout: const Duration(seconds: 5)),
      );

      final method = ClientMethod<String, String>(
        '/test.Echo/Say',
        (String v) => utf8.encode(v),
        (List<int> v) => utf8.decode(v),
      );

      final response = await client.$createUnaryCall(method, 'ping');
      expect(await response, 'pong:ping');
    });
  });
}
