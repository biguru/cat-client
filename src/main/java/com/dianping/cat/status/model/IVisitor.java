package com.dianping.cat.status.model;

import com.dianping.cat.status.model.entity.*;

public interface IVisitor {

   public void visitCustomInfo(CustomInfo customInfo);

   public void visitDisk(DiskInfo disk);

   public void visitDiskVolume(DiskVolumeInfo diskVolume);

   public void visitExtension(Extension extension);

   public void visitExtensionDetail(ExtensionDetail extensionDetail);

   public void visitGc(GcInfo gc);

   public void visitMemory(MemoryInfo memory);

   public void visitMessage(MessageInfo message);

   public void visitOs(OsInfo os);

   public void visitRuntime(RuntimeInfo runtime);

   public void visitStatus(StatusInfo status);

   public void visitThread(ThreadsInfo thread);
}
