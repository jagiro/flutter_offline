import 'dart:async';
import 'package:flutter/widgets.dart';
import 'package:connectivity_plus/connectivity_plus.dart';
import 'package:internet_connection_checker/internet_connection_checker.dart';

StreamTransformer<OfflineBuilderResult, OfflineBuilderResult> debounce(
  Duration debounceDuration,
) {
  var _seenFirstData = false;
  Timer? _debounceTimer;

  return StreamTransformer<OfflineBuilderResult,
      OfflineBuilderResult>.fromHandlers(
    handleData:
        (OfflineBuilderResult data, EventSink<OfflineBuilderResult> sink) {
      if (_seenFirstData) {
        _debounceTimer?.cancel();
        _debounceTimer = Timer(debounceDuration, () => sink.add(data));
      } else {
        sink.add(data);
        _seenFirstData = true;
      }
    },
    handleDone: (EventSink<OfflineBuilderResult> sink) {
      _debounceTimer?.cancel();
      sink.close();
    },
  );
}

StreamTransformer<ConnectivityResult, OfflineBuilderResult> startsWith(
  ConnectivityResult data,
) {
  return StreamTransformer<ConnectivityResult, OfflineBuilderResult>(
    (
      Stream<ConnectivityResult> input,
      bool cancelOnError,
    ) {
      StreamController<OfflineBuilderResult>? controller;
      late StreamSubscription<ConnectivityResult> subscription;

      controller = StreamController<OfflineBuilderResult>(
        sync: true,
        onListen: () async {
          final hasConnection = await InternetConnectionChecker().hasConnection;
          controller?.add(OfflineBuilderResult(data, hasConnection, AppLifecycleState.resumed == WidgetsBinding.instance.lifecycleState));
        },
        onPause: ([Future<dynamic>? resumeSignal]) =>
            subscription.pause(resumeSignal),
        onResume: () => subscription.resume(),
        onCancel: () => subscription.cancel(),
      );

      subscription = input.listen(
        (x) async {
          final hasConnection = await InternetConnectionChecker().hasConnection;
          controller?.add(OfflineBuilderResult(x, hasConnection, AppLifecycleState.resumed == WidgetsBinding.instance.lifecycleState));
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
