package com.barbarysoftware.watchservice;

import java.io.File;
import java.io.IOException;

import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;


public class WatchablePath implements Watchable {

    private final Path path;

    public WatchablePath(Path path) {
        if (path == null) {
            throw new NullPointerException("path must not be null");
        }
        this.path = path;
    }

    public Path getPath() {
        return path;
    }

    @Override
    public WatchKey register(WatchService watcher,
                             WatchEvent.Kind<?>[] events,
                             WatchEvent.Modifier... modifiers)
            throws IOException {
        if (watcher == null)
            throw new NullPointerException();
        if (!(watcher instanceof AbstractWatchService))
            throw new ProviderMismatchException();
        return ((AbstractWatchService) watcher).register(path, events, modifiers);
    }

    private static final WatchEvent.Modifier[] NO_MODIFIERS = new WatchEvent.Modifier[0];

    @Override
    public final WatchKey register(WatchService watcher,
                                   WatchEvent.Kind<?>... events)
            throws IOException {
        return register(watcher, events, NO_MODIFIERS);
    }

    @Override
    public String toString() {
        return "Path{" +
                "path=" + path +
                '}';
    }
}
