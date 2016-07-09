package com.alibaba.middleware.race.Tair;

import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;


/**
 * 读写tair所需要的集群信息，如masterConfigServer/slaveConfigServer地址/
 * group 、namespace我们都会在正式提交代码前告知选手
 */
public class TairOperatorImpl implements Serializable {
    public static int nameSpace;
    private static Logger LOG = LoggerFactory.getLogger(TairOperatorImpl.class);
    DefaultTairManager tairManager = new DefaultTairManager();

    public TairOperatorImpl() {
        tairManager.setGroupName(RaceConfig.TairGroup);
        if (RaceConfig.ONLINE) {
            this.nameSpace = RaceConfig.ONLINE_TAIR_NAMESPACE;
            tairManager.setConfigServerList(RaceConfig.ONLINE_TAIR_SERVER_ADDR);
        } else {
            this.nameSpace = RaceConfig.TAIR_NAMESPACE;
            tairManager.setConfigServerList(RaceConfig.TAIR_SERVER_ADDR);
        }
        tairManager.init();
    }

    public boolean write(Serializable key, Serializable value) {
        if (!RaceConfig.ONLINE)
            LOG.info("Tair: " + key +", " +value);

        ResultCode result = tairManager.put(nameSpace, key, value);
        return result.isSuccess();
    }

    public Object get(Serializable key) {
        Result<DataEntry> result = tairManager.get(nameSpace, key);
        if (result.isSuccess()) {
            DataEntry entry = result.getValue();
            if (entry == null) {
                return null;
            }
            return entry.getValue();
        } else {
            return "failed";
        }
    }

    public boolean remove(Serializable key) {
        tairManager.delete(nameSpace, key);
        return false;
    }

    public void close(){
        tairManager.close();
    }
}
