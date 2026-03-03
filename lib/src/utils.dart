import 'dart:async';
import 'package:flutter/widgets.dart';
import 'package:connectivity_plus/connectivity_plus.dart';
import 'package:internet_connection_checker_plus/internet_connection_checker_plus.dart';

StreamTransformer<OfflineBuilderResult, OfflineBuilderResult> debounce(
  Duration debounceDuration,
) {
  var seenFirstData = false;
  Timer? debounceTimer;

  return StreamTransformer<OfflineBuilderResult,
      OfflineBuilderResult>.fromHandlers(
    handleData:
        (OfflineBuilderResult data, EventSink<OfflineBuilderResult> sink) {
      if (seenFirstData) {
        debugPrint(
            '[OFFLINE_LIB] DEBOUNCE: waiting ${debounceDuration.inMilliseconds}ms before emitting hasConn=${data.hasConnection}, conn=${data.connectivityResult.name}');
        debounceTimer?.cancel();
        debounceTimer = Timer(debounceDuration, () {
          debugPrint(
              '[OFFLINE_LIB] DEBOUNCE: EMITTING hasConn=${data.hasConnection}, conn=${data.connectivityResult.name}');
          sink.add(data);
        });
      } else {
        debugPrint(
            '[OFFLINE_LIB] DEBOUNCE: first data, emitting immediately hasConn=${data.hasConnection}, conn=${data.connectivityResult.name}');
        sink.add(data);
        seenFirstData = true;
      }
    },
    handleDone: (EventSink<OfflineBuilderResult> sink) {
      debounceTimer?.cancel();
      sink.close();
    },
  );
}

StreamTransformer<List<ConnectivityResult>, OfflineBuilderResult> startsWith(
  List<ConnectivityResult> initialData,
  InternetConnection internetConnection,
) {
  return StreamTransformer<List<ConnectivityResult>, OfflineBuilderResult>(
    (
      Stream<List<ConnectivityResult>> input,
      bool cancelOnError,
    ) {
      StreamController<OfflineBuilderResult>? controller;
      late StreamSubscription<List<ConnectivityResult>> subscription;

      controller = StreamController<OfflineBuilderResult>(
        sync: true,
        onListen: () async {
          debugPrint(
              '[OFFLINE_LIB] STARTS_WITH onListen: checking internet...');
          final hasConnection =
              await internetConnection.hasInternetAccess;
          final ConnectivityResult primaryResult = initialData.isNotEmpty
              ? initialData.first
              : ConnectivityResult.none;

          debugPrint(
              '[OFFLINE_LIB] STARTS_WITH onListen: hasConn=$hasConnection, conn=$primaryResult');
          if (controller case final c? when !c.isClosed) {
            c.add(OfflineBuilderResult(
                primaryResult,
                hasConnection,
                AppLifecycleState.resumed ==
                    WidgetsBinding.instance.lifecycleState));
          }
        },
        onPause: ([Future<dynamic>? resumeSignal]) =>
            subscription.pause(resumeSignal),
        onResume: () => subscription.resume(),
        onCancel: () => subscription.cancel(),
      );

      subscription = input.listen(
        (List<ConnectivityResult> results) async {
          debugPrint(
              '[OFFLINE_LIB] STARTS_WITH onConnectivityChanged: $results, checking internet...');
          final hasConnection =
              await internetConnection.hasInternetAccess;
          final ConnectivityResult primaryResult =
              results.isNotEmpty ? results.first : ConnectivityResult.none;

          debugPrint(
              '[OFFLINE_LIB] STARTS_WITH onConnectivityChanged: hasConn=$hasConnection, conn=$primaryResult');
          if (controller case final c? when !c.isClosed) {
            c.add(OfflineBuilderResult(
                primaryResult,
                hasConnection,
                AppLifecycleState.resumed ==
                    WidgetsBinding.instance.lifecycleState));
          }
        },
        onError: controller.addError,
        onDone: controller.close,
        cancelOnError: cancelOnError,
      );

      return controller.stream.listen(null);
    },
  );
}

class OfflineBuilderResult {
  OfflineBuilderResult(
      this.connectivityResult, this.hasConnection, this.appIsResumed);

  final ConnectivityResult connectivityResult;
  final bool hasConnection;
  final bool appIsResumed;
}
