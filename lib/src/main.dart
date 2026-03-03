import 'dart:async';
import 'package:async/async.dart';
import 'package:connectivity_plus/connectivity_plus.dart';
import 'package:flutter/widgets.dart';
import 'package:flutter_offline/src/utils.dart';
import 'package:internet_connection_checker_plus/internet_connection_checker_plus.dart';
import 'package:network_info_plus/network_info_plus.dart';

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
    WidgetBuilder? errorBuilder,
    Duration? pingCheck,
    Widget? child,
    InternetConnection? internetConnection,
    bool? initialState,
  }) {
    return OfflineBuilder.initialize(
      key: key,
      connectivityBuilder: connectivityBuilder,
      connectivityService: Connectivity(),
      wifiInfo: NetworkInfo(),
      debounceDuration: debounceDuration,
      builder: builder,
      errorBuilder: errorBuilder,
      pingCheck: pingCheck,
      loadingWidget: loadingWidget,
      internetConnection: internetConnection,
      initialState: initialState,
      child: child,
    );
  }

  @visibleForTesting
  const OfflineBuilder.initialize({
    Key? key,
    required this.connectivityBuilder,
    required this.connectivityService,
    required this.wifiInfo,
    this.debounceDuration = kOfflineDebounceDuration,
    this.builder,
    this.errorBuilder,
    this.pingCheck,
    this.loadingWidget,
    this.child,
    this.internetConnection,
    this.initialState,
  })  : assert(
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

  /// Optional [InternetConnection] instance to use for connectivity checks.
  /// If not provided, uses the default [InternetConnection()] factory.
  final InternetConnection? internetConnection;

  /// Optional initial connectivity state (true = online, false = offline).
  /// When provided, the builder renders immediately with this value and
  /// skips the initial connectivity check. First real check happens at the
  /// next periodic interval or connectivity change event.
  final bool? initialState;

  @override
  OfflineBuilderState createState() => OfflineBuilderState();
}

class OfflineBuilderState extends State<OfflineBuilder> {
  late Stream<OfflineBuilderResult> connectivityStream;

  InternetConnection get _internetConnection =>
      widget.internetConnection ?? InternetConnection();

  @override
  void initState() {
    super.initState();

    final List<Stream<OfflineBuilderResult>> groupStreams = [];

    // Periodic stream: fires every pingCheck duration
    if (widget.pingCheck != null) {
      final tempPeriodicStream =
          Stream.periodic(widget.pingCheck!, (_) async {
        final List<ConnectivityResult> results =
            await widget.connectivityService.checkConnectivity();
        final ConnectivityResult connectivity =
            results.isNotEmpty ? results.first : ConnectivityResult.none;

        final bool hasConnection =
            await _internetConnection.hasInternetAccess;

        debugPrint(
            '[OFFLINE_LIB] PERIODIC check: hasConn=$hasConnection, conn=$connectivity, appResumed=${AppLifecycleState.resumed == WidgetsBinding.instance.lifecycleState}');

        return OfflineBuilderResult(
            connectivity,
            hasConnection,
            AppLifecycleState.resumed ==
                WidgetsBinding.instance.lifecycleState);
      }).asyncMap((event) => event);

      groupStreams.add(tempPeriodicStream);
    }

    // Connectivity change stream
    if (widget.initialState != null) {
      // Initial state provided: skip the initial check, only react to changes
      debugPrint(
          '[OFFLINE_LIB] Using initialState=${widget.initialState}, skipping initial check');
      final tempConnectivityStream = widget
          .connectivityService.onConnectivityChanged
          .asyncMap((List<ConnectivityResult> results) async {
        debugPrint(
            '[OFFLINE_LIB] onConnectivityChanged: $results, checking internet...');
        final hasConnection =
            await _internetConnection.hasInternetAccess;
        final ConnectivityResult primaryResult =
            results.isNotEmpty ? results.first : ConnectivityResult.none;
        debugPrint(
            '[OFFLINE_LIB] onConnectivityChanged result: hasConn=$hasConnection, conn=$primaryResult');
        return OfflineBuilderResult(
            primaryResult,
            hasConnection,
            AppLifecycleState.resumed ==
                WidgetsBinding.instance.lifecycleState);
      });
      groupStreams.add(tempConnectivityStream);
    } else {
      // No initial state: do the initial check (original behavior)
      final tempConnectivityStream =
          Stream.fromFuture(widget.connectivityService.checkConnectivity())
              .asyncExpand((data) {
        debugPrint(
            '[OFFLINE_LIB] CONNECTIVITY stream started, initial: $data');
        return widget.connectivityService.onConnectivityChanged
            .transform(startsWith(data, _internetConnection));
      });
      groupStreams.add(tempConnectivityStream);
    }

    // Debounce applied to the MERGED stream (covers both periodic and connectivity)
    connectivityStream = StreamGroup.merge(groupStreams)
        .transform(debounce(widget.debounceDuration));
  }

  @override
  void dispose() {
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    // When initialState is provided, render immediately without waiting for stream
    final initialData = widget.initialState != null
        ? OfflineBuilderResult(
            ConnectivityResult.other,
            widget.initialState!,
            true,
          )
        : null;

    return StreamBuilder<OfflineBuilderResult>(
      stream: connectivityStream,
      initialData: initialData,
      builder: (BuildContext context,
          AsyncSnapshot<OfflineBuilderResult> snapshot) {
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
