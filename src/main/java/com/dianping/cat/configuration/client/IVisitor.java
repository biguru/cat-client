package com.dianping.cat.configuration.client;

import com.dianping.cat.configuration.client.entity.*;

public interface IVisitor {

   public void visitBind(Bind bind);

   public void visitConfig(ClientConfig config);

   public void visitDomain(Domain domain);

   public void visitProperty(Property property);

   public void visitServer(Server server);
}
