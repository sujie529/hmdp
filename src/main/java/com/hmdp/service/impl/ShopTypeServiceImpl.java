package com.hmdp.service.impl;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import jakarta.annotation.Resource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;


import java.util.List;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_KEY;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryTypeList() {
        String key = CACHE_SHOP_TYPE_KEY;
        // 1. 从缓存中查询类型
        List<String> shopTypeJsonList = stringRedisTemplate.opsForList().range(key, 0, -1);
        // 2. 如果缓存中存在，返回缓存数据
        if (CollectionUtil.isNotEmpty(shopTypeJsonList)) {
            List<ShopType> shopTypes = shopTypeJsonList.stream()
                    .map(json -> JSONUtil.toBean(json, ShopType.class))
                    .collect(Collectors.toList());
            return Result.ok(shopTypes);
        }

        // 3. 如果缓存中不存在，查询数据库
        List<ShopType> shopTypes = query().orderByAsc("sort").list();
        // 4. 如果数据库中不存在，返回错误
        if (CollectionUtil.isEmpty(shopTypes)) {
            return Result.fail("店铺类型不存在");
        }
        //5.将数据写入redis
        List<String> shopTypesJson = shopTypes.stream()
                .map(shopType -> JSONUtil.toJsonStr(shopType))
                .collect(Collectors.toList());
        stringRedisTemplate.opsForList().rightPushAll(CACHE_SHOP_TYPE_KEY, shopTypesJson);
        //6.返回结果
        return Result.ok(shopTypes);
    }
}
