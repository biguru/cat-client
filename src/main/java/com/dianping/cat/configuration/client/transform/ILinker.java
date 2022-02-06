package com.dianping.cat.configuration.client.transform;

import com.dianping.cat.configuration.client.entity.*;

public interface ILinker {

   public boolean onBind(ClientConfig parent, Bind bind);

   public boolean onDomain(ClientConfig parent, Domain domain);

   public boolean onProperty(ClientConfig parent, Property property);

   public boolean onServer(ClientConfig parent, Server server);
}
