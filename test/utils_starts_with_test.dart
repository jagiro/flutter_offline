import 'dart:async';

import 'package:connectivity_plus/connectivity_plus.dart';
import 'package:flutter_offline/src/utils.dart' as transformers;
import 'package:flutter_test/flutter_test.dart';

void main() {
  // CAMBIADO: Ahora usa List<ConnectivityResult>
  StreamController<List<ConnectivityResult>> stream() =>
      StreamController<List<ConnectivityResult>>();

  late StreamController<List<ConnectivityResult>> values;
  late List<transformers.OfflineBuilderResult>
      emittedValues; // CAMBIADO: Ahora emite OfflineBuilderResult
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

    // CAMBIADO: Ahora el transformer maneja List<ConnectivityResult> -> OfflineBuilderResult
    subscription = values.stream
        .transform<transformers.OfflineBuilderResult>(transformer
            as StreamTransformer<List<ConnectivityResult>,
                transformers.OfflineBuilderResult>)
        .listen(emittedValues.add, onError: errors.add, onDone: () {
      // isDone = true;
    });
  }

  group('startWith', () {
    setUp(() {
      // CAMBIADO: Ahora pasa una lista con ConnectivityResult.none
      setupForStreamType(transformers.startsWith([ConnectivityResult.none]));
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
      await Future(() {});
      // CAMBIADO: Ahora verifica OfflineBuilderResult
      expect(emittedValues.length, 1);
      expect(emittedValues.first.connectivityResult, ConnectivityResult.none);
    });

    test('outputs all values', () async {
      values
        ..add([ConnectivityResult.mobile]) // CAMBIADO: Ahora son listas
        ..add([ConnectivityResult.wifi]);
      await Future(() {});

      // CAMBIADO: Verifica que tenemos 3 valores (inicial + 2 añadidos)
      expect(emittedValues.length, 3);
      expect(emittedValues[0].connectivityResult, ConnectivityResult.none);
      expect(emittedValues[1].connectivityResult, ConnectivityResult.mobile);
      expect(emittedValues[2].connectivityResult, ConnectivityResult.wifi);
    });

    test('outputs initial when followed by empty stream', () async {
      await values.close();
      expect(emittedValues.length, 1);
      expect(emittedValues.first.connectivityResult, ConnectivityResult.none);
    });

    test('handles multiple connectivity results', () async {
      // NUEVO: Test para manejar múltiples conexiones simultáneas
      values.add([ConnectivityResult.wifi, ConnectivityResult.mobile]);
      await Future(() {});

      expect(emittedValues.length, 2);
      // Debe tomar el primer resultado (wifi)
      expect(emittedValues.last.connectivityResult, ConnectivityResult.wifi);
    });

    test('handles empty connectivity results', () async {
      // NUEVO: Test para manejar lista vacía
      values.add([]);
      await Future(() {});

      expect(emittedValues.length, 2);
      // Con lista vacía debe devolver none
      expect(emittedValues.last.connectivityResult, ConnectivityResult.none);
    });
  });
}
