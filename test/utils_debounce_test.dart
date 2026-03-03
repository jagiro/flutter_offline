import 'dart:async';

import 'package:connectivity_plus/connectivity_plus.dart';
import 'package:flutter_offline/src/utils.dart';
import 'package:flutter_test/flutter_test.dart';

Future<void> waitForTimer(int milliseconds) => Future<void>(() {
      /* ensure Timer is started*/
    })
        .then<void>(
      (_) => Future<void>.delayed(Duration(milliseconds: milliseconds + 1)),
    );

void main() {
  StreamController<OfflineBuilderResult> stream() =>
      StreamController<OfflineBuilderResult>.broadcast();

  OfflineBuilderResult result(ConnectivityResult conn) =>
      OfflineBuilderResult(conn, false, true);

  group('Group', () {
    late StreamController<OfflineBuilderResult> values;
    late List<OfflineBuilderResult> emittedValues;
    late bool valuesCanceled;
    late bool isDone;
    late List errors;
    late StreamSubscription subscription;
    late Stream<OfflineBuilderResult> transformed;

    void setUpStreams(
        StreamTransformer<OfflineBuilderResult, OfflineBuilderResult>
            transformer) {
      valuesCanceled = false;
      values = stream()
        ..onCancel = () {
          valuesCanceled = true;
        };
      emittedValues = <OfflineBuilderResult>[];
      errors = <dynamic>[];
      isDone = false;
      transformed = values.stream.transform(transformer);
      subscription = transformed.listen(emittedValues.add,
          onError: errors.add, onDone: () {
        isDone = true;
      });
    }

    group('debounce', () {
      setUp(() async {
        setUpStreams(debounce(const Duration(milliseconds: 5)));
      });

      test('cancels values', () async {
        await subscription.cancel();
        expect(valuesCanceled, true);
      });

      test('swallows values that come faster than duration', () async {
        values.add(result(ConnectivityResult.mobile));
        values.add(result(ConnectivityResult.wifi));
        await values.close();
        await waitForTimer(5);
        expect(emittedValues.length, 1);
        expect(
            emittedValues.first.connectivityResult, ConnectivityResult.mobile);
      });

      test('outputs multiple values spaced further than duration', () async {
        values.add(result(ConnectivityResult.mobile));
        await waitForTimer(5);
        values.add(result(ConnectivityResult.wifi));
        await waitForTimer(5);
        expect(emittedValues.length, 2);
        expect(emittedValues[0].connectivityResult, ConnectivityResult.mobile);
        expect(emittedValues[1].connectivityResult, ConnectivityResult.wifi);
      });

      test('waits for pending value to close', () async {
        values.add(result(ConnectivityResult.mobile));
        await waitForTimer(5);
        await values.close();
        await Future(() {});
        expect(isDone, true);
      });
    });
  });
}
