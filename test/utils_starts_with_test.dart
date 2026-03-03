import 'dart:async';

import 'package:connectivity_plus/connectivity_plus.dart';
import 'package:flutter_offline/src/utils.dart' as transformers;
import 'package:flutter_test/flutter_test.dart';
import 'package:internet_connection_checker_plus/internet_connection_checker_plus.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();

  StreamController<List<ConnectivityResult>> stream() =>
      StreamController<List<ConnectivityResult>>();

  late StreamController<List<ConnectivityResult>> values;
  late List<transformers.OfflineBuilderResult> emittedValues;
  late bool valuesCanceled;
  late bool valuesPaused;
  late bool valuesResume;
  late StreamSubscription<transformers.OfflineBuilderResult> subscription;
  late List errors;

  void setupForStreamType(StreamTransformer transformer) {
    emittedValues = <transformers.OfflineBuilderResult>[];
    valuesCanceled = false;
    errors = <dynamic>[];
    values = stream()
      ..onPause = () {
        valuesPaused = true;
      }
      ..onResume = () {
        valuesResume = true;
      }
      ..onCancel = () {
        valuesCanceled = true;
      };

    subscription = values.stream
        .transform<transformers.OfflineBuilderResult>(transformer
            as StreamTransformer<List<ConnectivityResult>,
                transformers.OfflineBuilderResult>)
        .listen(emittedValues.add, onError: errors.add, onDone: () {});
  }

  // InternetConnection().hasInternetAccess is async (real HTTP check).
  // We need to wait long enough for it to complete in the test environment.
  Future<void> waitForInternetCheck() async {
    await Future.delayed(const Duration(seconds: 5));
  }

  group('startWith', () {
    setUp(() {
      setupForStreamType(transformers.startsWith(
          [ConnectivityResult.none], InternetConnection()));
    });

    test('cancels values', () async {
      await subscription.cancel();
      expect(valuesCanceled, true);
    });

    test('paused/resume values', () async {
      subscription.pause();
      expect(valuesPaused, true);
      subscription.resume();
      expect(valuesResume, true);
    });

    test('addError values', () async {
      values.addError(45);
      await Future(() {});
      expect(errors.length, isNonZero);
    });

    test('outputs initial value', () async {
      await waitForInternetCheck();
      expect(emittedValues.length, 1);
      expect(emittedValues.first.connectivityResult, ConnectivityResult.none);
    });

    test('outputs all values', () async {
      values
        ..add([ConnectivityResult.mobile])
        ..add([ConnectivityResult.wifi]);
      await waitForInternetCheck();

      expect(emittedValues.length, 3);
      expect(emittedValues[0].connectivityResult, ConnectivityResult.none);
      expect(emittedValues[1].connectivityResult, ConnectivityResult.mobile);
      expect(emittedValues[2].connectivityResult, ConnectivityResult.wifi);
    });

    test('outputs initial when followed by empty stream', () async {
      // Wait for the initial internet check to complete before closing
      await waitForInternetCheck();
      expect(emittedValues.length, 1);
      expect(emittedValues.first.connectivityResult, ConnectivityResult.none);
      await values.close();
    });

    test('handles multiple connectivity results', () async {
      values.add([ConnectivityResult.wifi, ConnectivityResult.mobile]);
      await waitForInternetCheck();

      expect(emittedValues.length, 2);
      expect(emittedValues.last.connectivityResult, ConnectivityResult.wifi);
    });

    test('handles empty connectivity results', () async {
      values.add([]);
      await waitForInternetCheck();

      expect(emittedValues.length, 2);
      expect(emittedValues.last.connectivityResult, ConnectivityResult.none);
    });
  });
}
