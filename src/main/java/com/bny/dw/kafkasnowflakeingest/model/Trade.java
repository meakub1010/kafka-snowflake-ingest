package com.bny.dw.kafkasnowflakeingest.model;

import java.math.BigDecimal;
import java.time.LocalDate;

//import com.fasterxml.jackson.annotation.JsonProperty;

public class Trade {
    // @JsonProperty("trade_date")
    private LocalDate tradeDate;
    private String fund;
    private String trader;
    private String security;
    private Integer quantity;
    private BigDecimal price;
}
