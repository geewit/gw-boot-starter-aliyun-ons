package com.aliyun.openservices.ons.api.impl.rocketmq;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.aliyun.openservices.ons.api.impl.authority.AuthUtil;
import com.aliyun.openservices.ons.api.impl.authority.SessionCredentials;

import static com.aliyun.openservices.ons.api.impl.authority.SessionCredentials.AccessKey;
import static com.aliyun.openservices.ons.api.impl.authority.SessionCredentials.ONSChannelKey;
import static com.aliyun.openservices.ons.api.impl.authority.SessionCredentials.SecurityToken;
import static com.aliyun.openservices.ons.api.impl.authority.SessionCredentials.Signature;

/**
 * @author lansheng.zj
 */
public class ClientRPCHook extends AbstractRPCHook {
    private SessionCredentials sessionCredentials;

    public ClientRPCHook(SessionCredentials sessionCredentials) {
        this.sessionCredentials = sessionCredentials;
    }

    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
        byte[] total = AuthUtil.combineRequestContent(request,
            parseRequestContent(request, sessionCredentials.getAccessKey(),
                sessionCredentials.getSecurityToken(), sessionCredentials.getOnsChannel().name()));
        String signature = AuthUtil.calSignature(total, sessionCredentials.getSecretKey());
        request.addExtField(Signature, signature);
        request.addExtField(AccessKey, sessionCredentials.getAccessKey());
        request.addExtField(ONSChannelKey, sessionCredentials.getOnsChannel().name());

        if (sessionCredentials.getSecurityToken() != null) {
            request.addExtField(SecurityToken, sessionCredentials.getSecurityToken());
        }
    }


    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {

    }

}
