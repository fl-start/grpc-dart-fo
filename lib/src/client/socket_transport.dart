// Copyright (c) 2025, the gRPC project authors. Please see the AUTHORS file
// for details. All rights reserved.

import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:http2/transport.dart';

import 'options.dart';
import 'proxy.dart';
import 'transport/http2_credentials.dart';

/// Opens a TCP socket to [host]:[port].
Future<Socket> openTcpSocket(
  Object host,
  int port, {
  Duration? connectTimeout,
}) async {
  return Socket.connect(host, port, timeout: connectTimeout);
}

/// An established transport plus the socket that actually backs it.
///
/// For TLS connections [socket] is the [SecureSocket] (not the raw TCP
/// socket), so callers can correctly track `done` and call `destroy()`.
/// After [SecureSocket.secure] the original raw socket must not be touched.
class ConnectedTransport {
  final ClientTransportConnection connection;
  final Socket socket;

  const ConnectedTransport(this.connection, this.socket);
}

/// Upgrades [socket] to TLS when [credentials] require it; returns the
/// transport together with the final socket backing it.
Future<ConnectedTransport> secureAndWrapTransport(
  Socket socket,
  ChannelOptions options, {
  required String tlsHost,
  required String authority,
}) async {
  var incoming = socket as Stream<List<int>>;
  final securityContext = options.credentials.securityContext;
  if (securityContext != null) {
    socket = await SecureSocket.secure(
      socket,
      host: tlsHost,
      context: securityContext,
      onBadCertificate: (certificate) =>
          _validateBadCertificate(options.credentials, certificate, authority),
    );
    incoming = socket;
  }

  if (socket.address.type != InternetAddressType.unix) {
    socket.setOption(SocketOption.tcpNoDelay, true);
  }

  return ConnectedTransport(
    ClientTransportConnection.viaStreams(incoming, socket),
    socket,
  );
}

bool _validateBadCertificate(
  ChannelCredentials credentials,
  X509Certificate certificate,
  String authority,
) {
  final validator = credentials.onBadCertificate;
  if (validator == null) return false;
  return validator(certificate, authority);
}

/// Computes gRPC :authority from host and port.
String makeAuthority(Object host, int port, ChannelOptions options) {
  if (options.credentials.authority != null) {
    return options.credentials.authority!;
  }
  final portSuffix = port == 443 ? '' : ':$port';
  if (host is String) {
    return '$host$portSuffix';
  }
  final address = host as InternetAddress;
  if (address.type == InternetAddressType.unix) {
    return 'localhost';
  }
  return '${address.host}$portSuffix';
}

/// TLS/SNI hostname (without port).
String tlsHostName(Object host, ChannelOptions options) {
  if (options.credentials.authority != null) {
    return options.credentials.authority!;
  }
  if (host is String) return host;
  final address = host as InternetAddress;
  if (address.type == InternetAddressType.unix) return 'localhost';
  return address.host;
}

/// Connect through HTTP CONNECT [proxy] to [targetHost]:[targetPort].
Future<Stream<List<int>>> connectViaProxy(
  Socket proxySocket,
  Object targetHost,
  int targetPort,
  Proxy proxy,
) async {
  final headers = {'Host': '$targetHost:$targetPort'};
  if (proxy.isAuthenticated) {
    final authStr = '${proxy.username}:${proxy.password}';
    final auth = base64Encode(utf8.encode(authStr));
    headers[HttpHeaders.proxyAuthorizationHeader] = 'Basic $auth';
  }

  final completer = Completer<void>();
  final intermediate = StreamController<List<int>>();

  proxySocket.listen(
    (event) {
      if (completer.isCompleted) {
        intermediate.sink.add(event);
      } else {
        _waitForProxyResponse(event, completer);
      }
    },
    onDone: intermediate.close,
    onError: intermediate.addError,
  );

  _sendProxyConnect(proxySocket, targetHost, targetPort, headers);
  await completer.future;
  return intermediate.stream;
}

void _sendProxyConnect(
  Socket socket,
  Object targetHost,
  int targetPort,
  Map<String, String> headers,
) {
  const linebreak = '\r\n';
  socket.write('CONNECT $targetHost:$targetPort HTTP/1.1');
  socket.write(linebreak);
  headers.forEach((key, value) {
    socket.write('$key: $value');
    socket.write(linebreak);
  });
  socket.write(linebreak);
}

void _waitForProxyResponse(Uint8List chunk, Completer<void> completer) {
  // Guard: data may arrive after the completer is already resolved (e.g. the
  // 200-response body bytes arrive as a second chunk).
  if (completer.isCompleted) return;
  final response = ascii.decode(chunk);
  if (response.startsWith('HTTP/1.1 200')) {
    completer.complete();
  } else {
    // Fix 6: use completeError instead of throw so the Future returned by
    // connectViaProxy actually fails. Throwing synchronously inside a
    // socket.listen callback propagates to the stream's onError handler but
    // never resolves the completer, causing a permanent hang.
    completer.completeError(
      TransportException('Error establishing proxy connection: $response'),
    );
  }
}

/// Builds transport from an already-connected [socket] (failover IP pin path).
///
/// Returns the [ConnectedTransport] so callers can track the final (possibly
/// TLS-upgraded) socket for `done`/`shutdown`.
Future<ConnectedTransport> connectPinnedSocket(
  Socket socket,
  Object logicalHost,
  int port,
  ChannelOptions options,
) async {
  final authority = makeAuthority(logicalHost, port, options);
  final tlsHost = tlsHostName(logicalHost, options);
  return secureAndWrapTransport(
    socket,
    options,
    tlsHost: tlsHost,
    authority: authority,
  );
}
