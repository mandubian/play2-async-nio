package com.barbarysoftware.watchservice;

import java.nio.file.Path;
import java.io.IOException;

public class Utils {
  public static WatchKey register(Path path, WatchService watchService, WatchEvent.Kind<?>... events) throws IOException{
    return ((AbstractWatchService)watchService).register(path, events);
  }

}