package com.dianping.cat.status.model.transform;

import com.dianping.cat.status.model.entity.*;

public interface ILinker {

   public boolean onCustomInfo(StatusInfo parent, CustomInfo customInfo);

   public boolean onDisk(StatusInfo parent, DiskInfo disk);

   public boolean onDiskVolume(DiskInfo parent, DiskVolumeInfo diskVolume);

   public boolean onExtension(StatusInfo parent, Extension extension);

   public boolean onExtensionDetail(Extension parent, ExtensionDetail extensionDetail);

   public boolean onGc(MemoryInfo parent, GcInfo gc);

   public boolean onMemory(StatusInfo parent, MemoryInfo memory);

   public boolean onMessage(StatusInfo parent, MessageInfo message);

   public boolean onOs(StatusInfo parent, OsInfo os);

   public boolean onRuntime(StatusInfo parent, RuntimeInfo runtime);

   public boolean onThread(StatusInfo parent, ThreadsInfo thread);
}
