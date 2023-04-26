import 'dart:async';
import 'package:async/async.dart';
import 'package:connectivity_plus/connectivity_plus.dart';
import 'package:flutter/widgets.dart';
import 'package:flutter_offline/src/utils.dart';
import 'package:internet_connection_checker/internet_connection_checker.dart';
import 'package:network_info_plus/network_info_plus.dart';
import 'dart:developer' as developer;

const kOfflineDebounceDuration = Duration(seconds: 3);

typedef ValueWidgetBuilder<T> = Widget Function(
    BuildContext context, T value, Widget child);

class OfflineBuilder extends StatefulWidget {
  factory OfflineBuilder({
    Key? key,
    required ValueWidgetBuilder<OfflineBuilderResult> connectivityBuilder,
    Duration debounceDuration = kOfflineDebounceDuration,
    Widget? loadingWidget,
    WidgetBuilder? builder,
    Widget? child,
    WidgetBuilder? errorBuilder,
    Duration? pingCheck,
  }) {
    return OfflineBuilder.initialize(
        key: key,
        connectivityBuilder: connectivityBuilder,
        connectivityService: Connectivity(),
        wifiInfo: NetworkInfo(),
        debounceDuration: debounceDuration,
        builder: builder,
        errorBuilder: errorBuilder,
        child: child,
        pingCheck: pingCheck,
        loadingWidget: loadingWidget);
  }

  @visibleForTesting
  const OfflineBuilder.initialize(
      {Key? key,
      required this.connectivityBuilder,
      required this.connectivityService,
      required this.wifiInfo,
      this.debounceDuration = kOfflineDebounceDuration,
      this.builder,
      this.child,
      this.errorBuilder,
      this.pingCheck,
      this.loadingWidget})
      : assert(
            !(builder is WidgetBuilder && child is Widget) &&
                !(builder == null && child == null),
            'You should specify either a builder or a child'),
        super(key: key);

  /// Override connectivity service used for testing
  final Connectivity connectivityService;

  final NetworkInfo wifiInfo;

  /// Debounce duration from epileptic network situations
  final Duration debounceDuration;

  /// Used for building the Offline and/or Online UI
  final ValueWidgetBuilder<OfflineBuilderResult> connectivityBuilder;

  /// Used for building the child widget
  final WidgetBuilder? builder;

  /// The widget below this widget in the tree.
  final Widget? child;

  /// Used for building the error widget incase of any platform errors
  final WidgetBuilder? errorBuilder;

  final Duration? pingCheck;

  final Widget? loadingWidget;

  @override
  OfflineBuilderState createState() => OfflineBuilderState();
}

class OfflineBuilderState extends State<OfflineBuilder> {
  late Stream<OfflineBuilderResult> _connectivityStream;

  @override
  void initState() {
    super.initState();

    final List<Stream<OfflineBuilderResult>> groupStreams = [];

    if (widget.pingCheck != null) {
      final tempPeriodicStream = Stream.periodic(widget.pingCheck!, (_) async {
        if (AppLifecycleState.resumed ==
            WidgetsBinding.instance.lifecycleState) {
          ConnectivityResult connectivity =
              await widget.connectivityService.checkConnectivity();
          final bool hasConnection =
              await InternetConnectionChecker().hasConnection;
          developer.log('Check offline connectivity $hasConnection',
              name: 'com.wesmartpark');
        }
        return null;
      }).asyncMap((event) => event);
      groupStreams.add(tempPeriodicStream);
    }

    final tempConnectivityStream =
        Stream.fromFuture(widget.connectivityService.checkConnectivity())
            .asyncExpand((data) => widget
                .connectivityService.onConnectivityChanged
                .transform(startsWith(data)))
            .transform(debounce(widget.debounceDuration));

    groupStreams.add(tempConnectivityStream);

    _connectivityStream = StreamGroup.merge(groupStreams);
  }

  @override
  void dispose() {
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return StreamBuilder<OfflineBuilderResult>(
      stream: _connectivityStream,
      builder:
          (BuildContext context, AsyncSnapshot<OfflineBuilderResult> snapshot) {
        if (!snapshot.hasData && !snapshot.hasError) {
          return widget.loadingWidget ?? const SizedBox();
        }

        if (snapshot.hasError) {
          if (widget.errorBuilder != null) {
            return widget.errorBuilder!(context);
          }
          throw OfflineBuilderError(snapshot.error!);
        }

        return widget.connectivityBuilder(
            context, snapshot.data!, widget.child ?? widget.builder!(context));
      },
    );
  }
}

class OfflineBuilderError extends Error {
  OfflineBuilderError(this.error);

  final Object error;

  @override
  String toString() => error.toString();
}
