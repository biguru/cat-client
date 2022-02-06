package com.dianping.cat.configuration.client.transform;

import com.dianping.cat.configuration.client.entity.*;

public interface IParser<T> {
   public ClientConfig parse(IMaker<T> maker, ILinker linker, T node);

   public void parseForBind(IMaker<T> maker, ILinker linker, Bind parent, T node);

   public void parseForDomain(IMaker<T> maker, ILinker linker, Domain parent, T node);

   public void parseForProperty(IMaker<T> maker, ILinker linker, Property parent, T node);

   public void parseForServer(IMaker<T> maker, ILinker linker, Server parent, T node);
}
