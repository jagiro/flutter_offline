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
        debounceTimer?.cancel();
        debounceTimer = Timer(debounceDuration, () => sink.add(data));
      } else {
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

// CORREGIDO: Ahora maneja List<ConnectivityResult> en lugar de ConnectivityResult
StreamTransformer<List<ConnectivityResult>, OfflineBuilderResult> startsWith(
  List<ConnectivityResult> initialData,
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
          final hasConnection = await InternetConnection().hasInternetAccess;
          // Usar el primer resultado de la lista como antes
          final ConnectivityResult primaryResult = initialData.isNotEmpty
              ? initialData.first
              : ConnectivityResult.none;

          controller?.add(OfflineBuilderResult(
              primaryResult,
              hasConnection,
              AppLifecycleState.resumed ==
                  WidgetsBinding.instance.lifecycleState));
        },
        onPause: ([Future<dynamic>? resumeSignal]) =>
            subscription.pause(resumeSignal),
        onResume: () => subscription.resume(),
        onCancel: () => subscription.cancel(),
      );

      subscription = input.listen(
        (List<ConnectivityResult> results) async {
          final hasConnection = await InternetConnection().hasInternetAccess;
          // Usar el primer resultado de la lista
          final ConnectivityResult primaryResult =
              results.isNotEmpty ? results.first : ConnectivityResult.none;

          controller?.add(OfflineBuilderResult(
              primaryResult,
              hasConnection,
              AppLifecycleState.resumed ==
                  WidgetsBinding.instance.lifecycleState));
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
