package com.barbarysoftware.watchservice;

import com.barbarysoftware.jna.*;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.nio.file.Paths;

/**
 * This class contains the bulk of my implementation of the Watch Service API. It hooks into Carbon's
 * File System Events API.
 *
 * @author Steve McLeod
 */
public class MacOSXListeningWatchService extends AbstractWatchService {

    // need to keep reference to callbacks to prevent garbage collection
    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
    private final List<CarbonAPI.FSEventStreamCallback> callbackList = new ArrayList<CarbonAPI.FSEventStreamCallback>();
    private final List<CFRunLoopThread> threadList = new ArrayList<CFRunLoopThread>();

    @Override
    public WatchKey register(Path path, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifers) throws IOException {
        final File file = path.toFile();
        final Map<Path, FileTime> lastModifiedMap = createLastModifiedMap(path);
        final String s = path.toAbsolutePath().toString();
        final Pointer[] values = {CFStringRef.toCFString(s).getPointer()};
        final CFArrayRef pathsToWatch = CarbonAPI.INSTANCE.CFArrayCreate(null, values, CFIndex.valueOf(1), null);
        final MacOSXWatchKey watchKey = new MacOSXWatchKey(this, new WatchablePath(path), events);

        final double latency = 1.0; /* Latency in seconds */

        final long kFSEventStreamEventIdSinceNow = -1; //  this is 0xFFFFFFFFFFFFFFFF
        final int kFSEventStreamCreateFlagNoDefer = 0x00000002;
        final CarbonAPI.FSEventStreamCallback callback = new MacOSXListeningCallback(watchKey, lastModifiedMap);
        callbackList.add(callback);
        final FSEventStreamRef stream = CarbonAPI.INSTANCE.FSEventStreamCreate(
                Pointer.NULL,
                callback,
                Pointer.NULL,
                pathsToWatch,
                kFSEventStreamEventIdSinceNow,
                latency,
                kFSEventStreamCreateFlagNoDefer);

        final CFRunLoopThread thread = new CFRunLoopThread(stream, file);
        thread.setDaemon(true);
        thread.start();
        threadList.add(thread);
        return watchKey;
    }

    public static class CFRunLoopThread extends Thread {

        private final FSEventStreamRef streamRef;
        private CFRunLoopRef runLoop;

        public CFRunLoopThread(FSEventStreamRef streamRef, File file) {
            super("WatchService for " + file);
            this.streamRef = streamRef;
        }

        @Override
        public void run() {
            runLoop = CarbonAPI.INSTANCE.CFRunLoopGetCurrent();
            final CFStringRef runLoopMode = CFStringRef.toCFString("kCFRunLoopDefaultMode");
            CarbonAPI.INSTANCE.FSEventStreamScheduleWithRunLoop(streamRef, runLoop, runLoopMode);
            CarbonAPI.INSTANCE.FSEventStreamStart(streamRef);
            CarbonAPI.INSTANCE.CFRunLoopRun();
        }

        public CFRunLoopRef getRunLoop() {
            return runLoop;
        }

        public FSEventStreamRef getStreamRef() {
            return streamRef;
        }
    }

    private Map<Path, FileTime> createLastModifiedMap(Path path) {
        Map<Path, FileTime> lastModifiedMap = new ConcurrentHashMap<Path, FileTime>();
        try {
            for (Path child : recursiveListFiles(path)) {
                //System.out.println("registering "+child.toRealPath() + " time:"+ Files.getLastModifiedTime(child));
                lastModifiedMap.put(child.toRealPath(), Files.getLastModifiedTime(child));
            }
        } catch(IOException ex) {
            ex.printStackTrace();
            // TODO manage this error
        }
        return lastModifiedMap;
    }

    private static Set<Path> recursiveListFiles(Path path) {
        Set<Path> paths = new HashSet<Path>();
        paths.add(path);
        try {
            if (Files.isDirectory(path)) {
                for (Path child : Files.newDirectoryStream(path)) {
                    paths.addAll(recursiveListFiles(child));
                }
            }
        } catch(IOException ex) {
            ex.printStackTrace();
            // TODO manage this error
        } 
        return paths;
    }

    @Override
    public void implClose() throws IOException {
        for (CFRunLoopThread thread : threadList) {
            CarbonAPI.INSTANCE.CFRunLoopStop(thread.getRunLoop());
            CarbonAPI.INSTANCE.FSEventStreamStop(thread.getStreamRef());
        }
        threadList.clear();
        callbackList.clear();
    }


    private static class MacOSXListeningCallback implements CarbonAPI.FSEventStreamCallback {
        private final MacOSXWatchKey watchKey;
        private final Map<Path, FileTime> lastModifiedMap;

        private MacOSXListeningCallback(MacOSXWatchKey watchKey, Map<Path, FileTime> lastModifiedMap) {
            this.watchKey = watchKey;
            this.lastModifiedMap = lastModifiedMap;
        }

        public void invoke(FSEventStreamRef streamRef, Pointer clientCallBackInfo, NativeLong numEvents, Pointer eventPaths, Pointer /* array of unsigned int */ eventFlags, /* array of unsigned long */ Pointer eventIds) {
            final int length = numEvents.intValue();
            try {
                for (String folderName : eventPaths.getStringArray(0, length)) {
                    final Set<Path> filesOnDisk = recursiveListFiles(Paths.get(folderName));

                    final List<Path> createdFiles = findCreatedFiles(filesOnDisk);
                    final List<Path> modifiedFiles = findModifiedFiles(filesOnDisk);
                    final List<Path> deletedFiles = findDeletedFiles(folderName, filesOnDisk);

                    for (Path path : createdFiles) {
                        if (watchKey.isReportCreateEvents()) {
                            watchKey.signalEvent(StandardWatchEventKinds.ENTRY_CREATE, path);
                        }
                        lastModifiedMap.put(path, Files.getLastModifiedTime(path));
                    }

                    for (Path path : modifiedFiles) {
                        if (watchKey.isReportModifyEvents()) {
                            watchKey.signalEvent(StandardWatchEventKinds.ENTRY_MODIFY, path);
                        }
                        lastModifiedMap.put(path, Files.getLastModifiedTime(path));
                    }

                    for (Path path : deletedFiles) {
                        if (watchKey.isReportDeleteEvents()) {
                            watchKey.signalEvent(StandardWatchEventKinds.ENTRY_DELETE, path);
                        }
                        lastModifiedMap.remove(path);
                    }
                }
            } catch(IOException ex) {
                ex.printStackTrace();
                // TODO manage this error
            }
        }

        private List<Path> findModifiedFiles(Set<Path> filesOnDisk) {
            List<Path> modifiedFileList = new ArrayList<Path>();
            try {
                for (Path path : filesOnDisk) {
                    final FileTime lastModified = lastModifiedMap.get(path);
                    //System.out.println("path:" + path + " lastmod known:"+ lastModified + " real:" + Files.getLastModifiedTime(path));
                    if (lastModified != null && !Files.getLastModifiedTime(path).equals(lastModified)) {
                        modifiedFileList.add(path);
                        //System.out.println("MODIFIED");
                    }
                }
                    //System.out.println("--------");
            } catch(IOException ex) {
                ex.printStackTrace();
                // TODO manage this error
            }
            return modifiedFileList;
        }

        private List<Path> findCreatedFiles(Set<Path> filesOnDisk) {
            List<Path> createdFileList = new ArrayList<Path>();
            for (Path file : filesOnDisk) {
                if (!lastModifiedMap.containsKey(file)) {
                    createdFileList.add(file);
                }
            }
            return createdFileList;
        }

        private List<Path> findDeletedFiles(String folderName, Set<Path> filesOnDisk) {
            List<Path> deletedFileList = new ArrayList<Path>();
            for (Path file : lastModifiedMap.keySet()) {
                if (file.toAbsolutePath().startsWith(folderName) && !filesOnDisk.contains(file)) {
                    deletedFileList.add(file);
                }
            }
            return deletedFileList;
        }
    }
}
