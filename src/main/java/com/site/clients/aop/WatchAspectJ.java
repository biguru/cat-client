package com.site.clients.aop;

import com.dianping.cat.Cat;
import com.dianping.cat.CatConstants;
import com.dianping.cat.message.Transaction;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.*;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;

@Aspect
@Component
public class WatchAspectJ {
    private static final Logger LOGGER = LoggerFactory.getLogger(WatchAspectJ.class);

    @Pointcut("@annotation(com.site.clients.aop.Watch)")
    public void pointCut() {
    }

    @Around("pointCut()")
    public Object around(ProceedingJoinPoint pjp) throws Throwable {
        if (!Cat.isInitialized()) {
            return pjp.proceed();
        }
        String className = pjp.getSignature().getDeclaringTypeName();
        Method method = ((MethodSignature) pjp.getSignature()).getMethod();

        Watch watch = (Watch) pjp.getSignature().getDeclaringType().getAnnotation(Watch.class);
        if (null == watch) {
            watch = method.getAnnotation(Watch.class);
        }

        Transaction transaction = null;
        try {
            if (null != watch) {
                transaction = Cat.newTransaction(CatConstants.TYPE_SERVICE_METHOD, className + "." + method.getName());
            }
            return pjp.proceed();
        } catch (Throwable e) {
            if (null != transaction) {
                Cat.logError(e);
            }
            throw e;
        } finally {
            if (null != transaction) {
                transaction.complete();
            }
        }
    }
}
