package com.alibaba.middleware.race.jstorm.blot;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.model.Ratio;
import com.alibaba.middleware.race.model.RatioProcess;
import com.alibaba.middleware.race.test.AnalyseThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wangwenfeng on 7/1/16.
 * 收到上游的tuple在没有endFlag的情况下, minuteTimeStamp肯定是不一样的. 所以需要每次判断当前接受到的Tuple是向前的还是向后的. 拿到对应
 * 的Ratio对象后, 判断其是不是null, 如果是, 则需要新建, 新建的时候, 需要找到这个Ratio对象的前一个节点, 然后将前一个节点的状态copy过来,
 * 然后判断前一个节点的nextRatio是否为空, 如果是, 则说明该节点是当前最后面一个节点, 不需要对已有的节点进行更新. 如果否, 用当前节点的nextRatio
 * 开始遍历, 将这个Tuple的的节点金额给后面的全部加上. 刷到Tair的策略是当minuteTimeStamp发生改变后进行刷盘, 并对nextRatio进行遍历.
 * 如果get到的Ratio不是null, 就需要开始对nextRatio进行遍历.
 */
public class PersistRatio implements IBasicBolt, Serializable {
    private static Logger LOG = LoggerFactory.getLogger(PersistRatio.class);
    private ConcurrentHashMap<Long,Ratio> ratioMap;
    private long currentTimeStamp;
    private long minTimeStamp;
    private long maxTimeStamp;
    private transient RatioProcess ratioProcess;

    private Ratio headNode;
    private Ratio tailNode;

    private boolean endFlag;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        ratioMap = new ConcurrentHashMap<>();
        currentTimeStamp = -1;
        ratioProcess = new RatioProcess();
        endFlag = false;
        headNode = tailNode = null;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        long minuteTimeStamp = (Long) input.getValue(0);
        // input.getValue(1) 他妈的这个地方这个对象是复用的！！！在没有收到endFlag的时候，一直引用的是RatioCount中的sumAmout WHAT THE FUCK!
        double[] amount = (double[]) input.getValue(1); // 0 PC 1 MOBILE

        Ratio ratioNode = ratioMap.get(minuteTimeStamp);
        if (endFlag) {
            ratioProcess.updateAmount(ratioNode, amount);
            Ratio _ratio = ratioMap.get(minuteTimeStamp);
            ratioProcess.updateRatio(_ratio);
            return;
        }

        if (ratioNode == null) {
            if (currentTimeStamp == -1) { // 0 init
                ratioNode = new Ratio(minuteTimeStamp,null);
                maxTimeStamp = minTimeStamp = currentTimeStamp = minuteTimeStamp;
                headNode = tailNode = ratioNode;
            } else if (minuteTimeStamp < minTimeStamp) {
                ratioNode = new Ratio(minuteTimeStamp,headNode,0); // flag 位，0是head节点 ，1是tail节点
                minTimeStamp = minuteTimeStamp;
                headNode = ratioNode;
            } else if (minuteTimeStamp > maxTimeStamp) {
                ratioNode = new Ratio(minuteTimeStamp,tailNode,1); // flag 位，0是head节点 ，1是tail节点
                maxTimeStamp = minuteTimeStamp;
                tailNode = ratioNode;
            } else {
                ratioNode = ratioMap.get(maxTimeStamp);
                boolean finded = false;
                while (ratioNode.getPreRatio()!=null) {
                    if (minuteTimeStamp < ratioNode.getTimeStamp() && minuteTimeStamp > ratioNode.getPreRatio().getTimeStamp()) {
                        ratioNode = new Ratio(minuteTimeStamp,ratioNode.getPreRatio());
                        finded = true;
                        break;
                    }
                    ratioNode =ratioNode.getPreRatio();
                }
                if (!finded) {
                    throw new RuntimeException("Not find PreRatio!!!");
                }
            }
            ratioMap.put(minuteTimeStamp,ratioNode);
        }

        ratioProcess.updateAmount(ratioNode, amount);
        if (minuteTimeStamp != currentTimeStamp) {

            if (minuteTimeStamp == 0 && amount[0] == 0 && amount[1] == 0 ) {
                if (!RaceConfig.ONLINE) {
                    try {
                        new Thread(new AnalyseThread(RaceConfig.PY_LOG_PATH,3)).start();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                Ratio _ratio = ratioMap.get(currentTimeStamp);
                if (_ratio != null) {
                    ratioProcess.updateRatio(_ratio);
                }
                endFlag = true;
                return;
            }

            // 如果不等式成立,并且endflag了
            Ratio _ratio = ratioMap.get(currentTimeStamp);
            if (_ratio != null) {
                ratioProcess.updateRatio(_ratio);
            }
            currentTimeStamp = minuteTimeStamp;
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
