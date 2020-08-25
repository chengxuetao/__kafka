package com.example.kafka.zmart;

import org.apache.kafka.common.serialization.Serde;

import com.example.kafka.zmart.model.Purchase;
import com.example.kafka.zmart.model.PurchasePattern;
import com.example.kafka.zmart.model.RewardAccumulator;

public class StreamsSerdes {

    public static Serde<Purchase> purchaseSerde() {
        return new PurchaseSerde();
    }

    public static Serde<PurchasePattern> purchasePatternSerde() {
        return new PurchasePatternsSerde();
    }

    public static Serde<RewardAccumulator> rewardAccumulatorSerde() {
        return new RewardAccumulatorSerde();
    }

    public static final class PurchaseSerde extends WrapperSerde<Purchase> {
        public PurchaseSerde() {
            super(new JsonSerializer<Purchase>(), new JsonDeserializer<Purchase>(Purchase.class));
        }
    }

    public static final class PurchasePatternsSerde extends WrapperSerde<PurchasePattern> {
        public PurchasePatternsSerde() {
            super(new JsonSerializer<PurchasePattern>(), new JsonDeserializer<PurchasePattern>(PurchasePattern.class));
        }
    }

    public static final class RewardAccumulatorSerde extends WrapperSerde<RewardAccumulator> {
        public RewardAccumulatorSerde() {
            super(new JsonSerializer<RewardAccumulator>(),
                new JsonDeserializer<RewardAccumulator>(RewardAccumulator.class));
        }
    }

}
