package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;


    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private volatile boolean running=true;
    private ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    @PreDestroy
    private void destroy() {
        running = false;
        SECKILL_ORDER_EXECUTOR.shutdown();
        log.info("秒杀订单线程已停止");
    }

    private class VoucherOrderHandler implements Runnable {
        String queueName = "stream.orders";
        @Override
        public void run() {
            // 初始化消费者组
            try {
                // 创建消费者组，如果组已存在则忽略错误
                stringRedisTemplate.opsForStream().createGroup(queueName, "g1");
            } catch (Exception e) {
                log.warn("消费者组 g1 已存在，跳过创建");
            }
            while (running) {
                try {
                    //1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.order >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //2判断消息获取是否成功
                    if (list == null || list.isEmpty()) {
                        //2.1.如果获取失败，说明没有消息，继续下一次循环
                        continue;
                    }
                    //3.解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //3.获取成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    //4.ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    // 终极方案：只要是 Redis 连接关闭/停止，直接安静退出，不报错
                    if (!running) {
                        // 服务正在关闭，直接退出，不打印日志
                        break;
                    }
                    log.error("处理订单异常", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList()  {
            while (running) {
                try {
                    //1.获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.order >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //2判断消息获取是否成功
                    if (list == null || list.isEmpty()) {
                        //2.1.如果获取失败，说明pending-list没有消息，结束循环
                        break;
                    }
                    //3.解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //3.获取成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    //4.ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    if (!running) {
                        break;
                    }
                    log.error("处理pending-list订单异常", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
    }

//    private BlockingQueue<VoucherOrder> orderTasks=new ArrayBlockingQueue<>(1024*1024);
//    private class VoucherOrderHandler implements Runnable {
//
//        @Override
//        public void run() {
//            while (true) {
//                //1.获取队列中的订单信息
//                try {
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    //2.创建订单
//                    handleVoucherOrder(voucherOrder);
//                } catch (Exception e) {
//                    log.error("处理订单异常", e);
//                }
//            }
//        }
//    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
            //1.获取用户
            Long userId = voucherOrder.getUserId();
            //创建锁对象
            RLock lock = redissonClient.getLock("lock:order:" + userId);
            //获取锁
            boolean isLocked = lock.tryLock();
            if (!isLocked) {
                //获取锁失败
                log.error("请勿重复下单");
                return;
            }
            try {
                //获取代理对象(事务)

                 proxy.createVoucherOrder(voucherOrder);
            }finally {
                //释放锁
                lock.unlock();
            }
    }
    private IVoucherOrderService proxy;

    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        //获取订单id
        long orderId = redisIdWorker.nextId("order");
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId)
        );
        //2.判断结果是否为0
        int r = result.intValue();
        if (r != 0) {
            //2.1.不为0，没有购买资格
            return Result.fail(r==1?"库存不足":"不能重复下单");
        }

        //获取代理对象(事务)
        proxy=(IVoucherOrderService) AopContext.currentProxy();
        //3.返回订单id
        return Result.ok(orderId);
    }

    /**
     *
     * @param voucherOrder
     */
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //获取用户
//        Long userId = UserHolder.getUser().getId();
//        //1.执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(),
//                userId.toString()
//        );
//        //2.判断结果是否为0
//        int r = result.intValue();
//        if (r != 0) {
//            //2.1.不为0，没有购买资格
//            return Result.fail(r==1?"库存不足":"不能重复下单");
//        }
//        //2.2.为0，有购买资格,把下单信息保存到阻塞队列
//
//        VoucherOrder voucherOrder = new VoucherOrder();
//        //2.3 订单id
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        //2.4用户id
//        voucherOrder.setUserId(userId);
//        //2.5代金券id
//        voucherOrder.setVoucherId(voucherId);
//        //2.6放入阻塞队列
//        orderTasks.add(voucherOrder);
//
//        //获取代理对象(事务)
//        proxy=(IVoucherOrderService) AopContext.currentProxy();
//        //3.返回订单id
//        return Result.ok(orderId);
//    }
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //1.查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        //2.判断秒杀是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            //秒杀未开始
//            return Result.fail("秒杀尚未开始");
//        }
//        //3.判断秒杀是否已经结束
//        if(voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            //秒杀已结束
//            return Result.fail("秒杀已结束");
//        }
//        //4.判断库存是否充足
//        if (voucher.getStock() <1){
//            //库存不足
//            return Result.fail("库存不足!");
//        }
//
//        Long userId = UserHolder.getUser().getId();
//        //创建锁对象
//        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        //获取锁
//        boolean isLocked = lock.tryLock();
//        if (!isLocked) {
//            //获取锁失败
//            return Result.fail("请勿重复下单");
//        }
//        try {
//            //获取代理对象(事务)
//            IVoucherOrderService proxy=(IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        }finally {
//            //释放锁
//            lock.unlock();
//        }
//
//    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder){
        //一人一单
        Long userId = voucherOrder.getUserId();

            //查询订单
        Long count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
            //判断是否存在
            if (count > 0) {
                //用户已经购买过了
                log.error("用户已经购买过一次了");
                return;
            }

            //5.扣减库存
            boolean success = seckillVoucherService.update().
                    setSql("stock = stock - 1").
                    eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0)
                    .update();
            if (!success) {
                //扣减失败
                log.error("库存不足!");
                return;
            }



            save(voucherOrder);

    }
}
