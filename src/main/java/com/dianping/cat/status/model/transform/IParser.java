package com.dianping.cat.status.model.transform;

import com.dianping.cat.status.model.entity.*;

public interface IParser<T> {
   public StatusInfo parse(IMaker<T> maker, ILinker linker, T node);

   public void parseForCustomInfo(IMaker<T> maker, ILinker linker, CustomInfo parent, T node);

   public void parseForDiskInfo(IMaker<T> maker, ILinker linker, DiskInfo parent, T node);

   public void parseForDiskVolumeInfo(IMaker<T> maker, ILinker linker, DiskVolumeInfo parent, T node);

   public void parseForExtension(IMaker<T> maker, ILinker linker, Extension parent, T node);

   public void parseForExtensionDetail(IMaker<T> maker, ILinker linker, ExtensionDetail parent, T node);

   public void parseForGcInfo(IMaker<T> maker, ILinker linker, GcInfo parent, T node);

   public void parseForMemoryInfo(IMaker<T> maker, ILinker linker, MemoryInfo parent, T node);

   public void parseForMessageInfo(IMaker<T> maker, ILinker linker, MessageInfo parent, T node);

   public void parseForOsInfo(IMaker<T> maker, ILinker linker, OsInfo parent, T node);

   public void parseForRuntimeInfo(IMaker<T> maker, ILinker linker, RuntimeInfo parent, T node);

   public void parseForThreadsInfo(IMaker<T> maker, ILinker linker, ThreadsInfo parent, T node);
}
