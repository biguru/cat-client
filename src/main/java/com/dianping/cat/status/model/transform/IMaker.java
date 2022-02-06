package com.dianping.cat.status.model.transform;

import com.dianping.cat.status.model.entity.*;

public interface IMaker<T> {

   public CustomInfo buildCustomInfo(T node);

   public DiskInfo buildDisk(T node);

   public DiskVolumeInfo buildDiskVolume(T node);

   public Extension buildExtension(T node);

   public ExtensionDetail buildExtensionDetail(T node);

   public GcInfo buildGc(T node);

   public MemoryInfo buildMemory(T node);

   public MessageInfo buildMessage(T node);

   public OsInfo buildOs(T node);

   public RuntimeInfo buildRuntime(T node);

   public StatusInfo buildStatus(T node);

   public ThreadsInfo buildThread(T node);
}
