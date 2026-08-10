// ============================================================================
// PART 2 — TOP-50 TRIGGER / TECHNICAL / ALERT / SCAN LOOP
// ============================================================================

function computeTop20MomentumQuality({
  leader,
  bars,
  macd,
  sma10,
  sma100,
  sma100Up,
}) {
  if (
    !TOP20_QUALITY_ENABLED ||
    !bars?.length ||
    !sma10 ||
    !sma100
  ) {
    return {
      score: 0,
      label: "UNAVAILABLE",
      riskFlags: [],
      metrics: {},
    };
  }

  const last = bars[bars.length - 1];
  const price = Number(leader.price || last.c || 0);

  let trendScore = 0;
  if (sma10 > sma100) trendScore += 10;
  if (sma100Up) trendScore += 10;

  let macdScore = 0;
  if (
    macd?.macd > macd?.signal &&
    macd?.histogram > 0
  ) {
    macdScore += 10;
  }

  if (macd?.strengthening) {
    macdScore += 10;
  }

  const recent3 = bars.slice(-3);
  const prior10 = bars.slice(-13, -3);

  const recent3Avg =
    recent3.length
      ? recent3.reduce((s, b) => s + b.v, 0) / recent3.length
      : 0;

  const prior10Avg =
    prior10.length
      ? prior10.reduce((s, b) => s + b.v, 0) / prior10.length
      : 0;

  const volumeAcceleration =
    prior10Avg > 0
      ? recent3Avg / prior10Avg
      : 1;

  const volumeScore =
    volumeAcceleration >= 1.5 ? 20 :
    volumeAcceleration >= 1.0 ? 12 :
    volumeAcceleration >= 0.7 ? 5 : 0;

  const distanceAboveSma10Pct =
    sma10 > 0
      ? ((price - sma10) / sma10) * 100
      : 0;

  let extensionScore =
    distanceAboveSma10Pct <= 1 ? 15 :
    distanceAboveSma10Pct <= 3 ? 12 :
    distanceAboveSma10Pct <= 6 ? 7 :
    distanceAboveSma10Pct <= 10 ? 2 : 0;

  if (price < sma10) {
    extensionScore = 3;
  }

  const prior20 = bars.slice(-21, -1);

  const recentHigh =
    prior20.length
      ? Math.max(...prior20.map(b => b.h))
      : last.h;

  const highRatio =
    recentHigh > 0
      ? price / recentHigh
      : 0;

  const breakoutScore =
    highRatio >= 1.0 ? 10 :
    highRatio >= 0.995 ? 8 :
    highRatio >= 0.98 ? 5 : 0;

  const last5 = bars.slice(-5);

  const totalVol5 =
    last5.reduce((s, b) => s + b.v, 0);

  const redVol5 =
    last5
      .filter(b => b.c < b.o)
      .reduce((s, b) => s + b.v, 0);

  const redVolumeRatio =
    totalVol5 > 0
      ? redVol5 / totalVol5
      : 0;

  const sellingPressureScore =
    redVolumeRatio <= 0.35 ? 10 :
    redVolumeRatio <= 0.50 ? 7 :
    redVolumeRatio <= 0.65 ? 3 : 0;

  const recentForReclaim =
    bars.slice(-5);

  const hadPullback =
    recentForReclaim
      .slice(0, -1)
      .some(b => b.l <= sma10);

  const reclaim =
    hadPullback &&
    last.c > sma10 &&
    last.c > last.o;

  const reclaimScore =
    reclaim ? 5 : 0;

  const riskFlags = [];

  if (distanceAboveSma10Pct > 6) {
    riskFlags.push("EXTENDED_FROM_10SMA");
  }

  if (volumeAcceleration < 0.7) {
    riskFlags.push("VOLUME_FADING");
  }

  if (macd && !macd.strengthening) {
    riskFlags.push("MACD_WEAKENING");
  }

  if (redVolumeRatio > 0.65) {
    riskFlags.push("HEAVY_RED_VOLUME");
  }

  if (Number(leader.pct || 0) >= 80) {
    riskFlags.push("DAY_MOVE_EXTENDED");
  }

  if (price < sma10) {
    riskFlags.push("BELOW_10SMA");
  }

  const rawScore =
    trendScore +
    macdScore +
    volumeScore +
    extensionScore +
    breakoutScore +
    sellingPressureScore +
    reclaimScore;

  let penalty = 0;

  if (riskFlags.includes("HEAVY_RED_VOLUME")) penalty += 25;
  if (riskFlags.includes("BELOW_10SMA")) penalty += 25;
  if (riskFlags.includes("VOLUME_FADING")) penalty += 15;
  if (riskFlags.includes("MACD_WEAKENING")) penalty += 10;
  if (riskFlags.includes("DAY_MOVE_EXTENDED")) penalty += 10;
  if (riskFlags.includes("EXTENDED_FROM_10SMA")) penalty += 10;

  const score =
    Math.round(
      clamp(
        rawScore - penalty,
        0,
        100
      )
    );

  let label =
    score >= TOP20_QUALITY_STRONG ? "STRONG" :
    score >= TOP20_QUALITY_CAUTION ? "GOOD" :
    score >= 40 ? "CAUTION" :
    "WEAK";

  if (
    riskFlags.includes("HEAVY_RED_VOLUME") ||
    riskFlags.includes("BELOW_10SMA")
  ) {
    label =
      score >= 40
        ? "CAUTION"
        : "WEAK";
  }

  return {
    score,
    rawScore: Math.round(rawScore),
    penalty,
    label,
    riskFlags,
    metrics: {
      volumeAcceleration:
        Number(volumeAcceleration.toFixed(3)),
      distanceAboveSma10Pct:
        Number(distanceAboveSma10Pct.toFixed(3)),
      redVolumeRatio:
        Number(redVolumeRatio.toFixed(3)),
      recentHigh:
        Number(recentHigh.toFixed(4)),
      nearRecentHigh:
        highRatio >= 0.98,
      breakout:
        highRatio >= 1.0,
      reclaim10Sma:
        reclaim,
      macdStrengthening:
        Boolean(macd?.strengthening),
    },
  };
}

function evaluateTop20Safety({
  leader,
  bars,
  macd,
  sma10,
  momentumQuality,
}) {
  if (!TOP20_SAFETY_FILTER_ENABLED) {
    return {
      passed: true,
      reasons: [],
      metrics: {},
    };
  }

  const last = bars[bars.length - 1];
  const closes = bars.map(b => b.c);
  const price = Number(leader.price || last.c || 0);

  const sma10PastEnd =
    closes.length - TOP20_SMA10_TREND_LOOKBACK;

  const pastSma10 =
    sma(closes, 10, sma10PastEnd);

  const sma10SlopePct =
    sma10 != null &&
    pastSma10 != null &&
    pastSma10 !== 0
      ? ((sma10 - pastSma10) / pastSma10) * 100
      : 0;

  const momentumLookback =
    Math.max(1, TOP20_RECENT_MOMENTUM_LOOKBACK);

  const oldIndex =
    Math.max(
      0,
      closes.length - 1 - momentumLookback
    );

  const oldClose =
    closes[oldIndex] || price;

  const recentMomentumPct =
    oldClose > 0
      ? ((last.c - oldClose) / oldClose) * 100
      : 0;

  const last3 =
    bars.slice(-3);

  const upperWickRatios =
    last3.map(b => {
      const range =
        Math.max(0, b.h - b.l);

      if (range <= 0) return 0;

      const upperWick =
        Math.max(
          0,
          b.h - Math.max(b.o, b.c)
        );

      return upperWick / range;
    });

  const avgUpperWickRatio =
    upperWickRatios.length
      ? upperWickRatios.reduce((a, b) => a + b, 0) /
        upperWickRatios.length
      : 0;

  const q =
    momentumQuality?.metrics || {};

  const redVolumeRatio =
    Number(q.redVolumeRatio || 0);

  const distanceAboveSma10Pct =
    Number(q.distanceAboveSma10Pct || 0);

  const reasons = [];

  if (
    TOP20_REQUIRE_PRICE_ABOVE_SMA10 &&
    !(price > sma10)
  ) {
    reasons.push("PRICE_BELOW_10SMA");
  }

  if (
    TOP20_REQUIRE_SMA10_RISING &&
    !(sma10SlopePct > 0)
  ) {
    reasons.push("SMA10_NOT_RISING");
  }

  if (
    redVolumeRatio >
    TOP20_MAX_RED_VOLUME_RATIO
  ) {
    reasons.push("HEAVY_RED_VOLUME");
  }

  if (
    recentMomentumPct <
    TOP20_MIN_RECENT_MOMENTUM_PCT
  ) {
    reasons.push("RECENT_MOMENTUM_WEAK");
  }

  if (
    avgUpperWickRatio >
    TOP20_MAX_UPPER_WICK_RATIO
  ) {
    reasons.push("REJECTION_WICKS");
  }

  if (
    distanceAboveSma10Pct >
    TOP20_MAX_DISTANCE_ABOVE_SMA10_PCT
  ) {
    reasons.push("TOO_EXTENDED_FROM_10SMA");
  }

  if (
    TOP20_REQUIRE_MACD_STRENGTHENING &&
    !macd?.strengthening
  ) {
    reasons.push("MACD_NOT_STRENGTHENING");
  }

  return {
    passed: reasons.length === 0,
    reasons,
    metrics: {
      priceAboveSma10:
        price > sma10,

      sma10SlopePct:
        Number(sma10SlopePct.toFixed(4)),

      recentMomentumPct:
        Number(recentMomentumPct.toFixed(4)),

      avgUpperWickRatio:
        Number(avgUpperWickRatio.toFixed(4)),
    },
  };
}

// ============================================================================
// >=5X COMPLETED 1-MINUTE CONFIRMATION
// ============================================================================

function evaluateTop20Trigger(bars) {
  const empty = {
    enabled: TOP20_TRIGGER_ENABLED,
    passed: false,
    uptrendConfirmed: false,
    volumeJumpPassed: false,
    multiplier: 0,
    currentCandleVolume: 0,
    previousCandleVolume: 0,
    triggerCandleTimestampMs: null,
    triggerCandleGreen: false,
    higherClose: false,
    priceAboveSma10: false,
    sma10Above100: false,
    sma10Rising: false,
    sma100Rising: false,
    sma10: null,
    sma100: null,
    reason: "UNAVAILABLE",
  };

  if (!TOP20_TRIGGER_ENABLED) {
    return {
      ...empty,
      reason: "DISABLED",
    };
  }

  if (
    !Array.isArray(bars) ||
    bars.length < 110
  ) {
    return {
      ...empty,
      reason: "INSUFFICIENT_BARS",
    };
  }

  // getTop20MinuteBars() already applies the grace period and strips the
  // forming candle. Do NOT subtract another full minute here.
  const completed =
    [...bars].sort((a, b) => a.t - b.t);

  if (completed.length < 110) {
    return {
      ...empty,
      reason: "INSUFFICIENT_COMPLETED_BARS",
    };
  }

  const current =
    completed[completed.length - 1];

  const previous =
    completed[completed.length - 2];

  const closes =
    completed.map(b => Number(b.c));

  const currentSma10 =
    sma(closes, 10);

  const currentSma100 =
    sma(closes, 100);

  const sma10PastEnd =
    Math.max(
      10,
      closes.length -
      TOP20_SMA10_TREND_LOOKBACK
    );

  const sma100PastEnd =
    Math.max(
      100,
      closes.length -
      TOP20_SMA_TREND_LOOKBACK
    );

  const pastSma10 =
    sma(
      closes,
      10,
      sma10PastEnd
    );

  const pastSma100 =
    sma(
      closes,
      100,
      sma100PastEnd
    );

  const priceAboveSma10 =
    currentSma10 != null &&
    Number(current.c) >
      currentSma10;

  const sma10Above100 =
    currentSma10 != null &&
    currentSma100 != null &&
    currentSma10 >
      currentSma100;

  const sma10Rising =
    currentSma10 != null &&
    pastSma10 != null &&
    currentSma10 >
      pastSma10;

  const sma100Rising =
    currentSma100 != null &&
    pastSma100 != null &&
    currentSma100 >
      pastSma100;

  const triggerCandleGreen =
    Number(current.c) >
    Number(current.o);

  const higherClose =
    Number(current.c) >
    Number(previous.c);

  const previousVolume =
    Number(previous.v || 0);

  const currentVolume =
    Number(current.v || 0);

  const multiplier =
    previousVolume > 0
      ? currentVolume /
        previousVolume
      : 0;

  // IMPORTANT: >= means 5x OR HIGHER.
  const volumeJumpPassed =
    multiplier >=
    TOP20_TRIGGER_VOLUME_MULTIPLIER;

  // Basic trend gate. SMA100 rising can be turned off in Railway so an early
  // runner is not ignored just because the slow average has not caught up yet.
  const uptrendConfirmed =
    Boolean(
      sma10Above100 &&

      (
        !TOP20_TRIGGER_REQUIRE_PRICE_ABOVE_SMA10 ||
        priceAboveSma10
      ) &&

      (
        !TOP20_TRIGGER_REQUIRE_SMA10_RISING ||
        sma10Rising
      ) &&

      (
        !TOP20_TRIGGER_REQUIRE_SMA100_RISING ||
        sma100Rising
      )
    );

  const passed =
    Boolean(
      uptrendConfirmed &&
      volumeJumpPassed &&

      (
        !TOP20_TRIGGER_REQUIRE_GREEN ||
        triggerCandleGreen
      ) &&

      (
        !TOP20_TRIGGER_REQUIRE_HIGHER_CLOSE ||
        higherClose
      )
    );

  let reason = "PASS";

  if (!uptrendConfirmed) {
    reason = "UPTREND_NOT_CONFIRMED";
  } else if (!volumeJumpPassed) {
    reason = "NO_5X_PREVIOUS_CANDLE_JUMP";
  } else if (
    TOP20_TRIGGER_REQUIRE_GREEN &&
    !triggerCandleGreen
  ) {
    reason = "TRIGGER_CANDLE_NOT_GREEN";
  } else if (
    TOP20_TRIGGER_REQUIRE_HIGHER_CLOSE &&
    !higherClose
  ) {
    reason = "TRIGGER_CANDLE_NOT_HIGHER_CLOSE";
  }

  return {
    enabled: true,
    passed,
    uptrendConfirmed,
    volumeJumpPassed,
    multiplier:
      Number(multiplier.toFixed(3)),
    currentCandleVolume:
      Math.round(currentVolume),
    previousCandleVolume:
      Math.round(previousVolume),
    triggerCandleTimestampMs:
      Number(current.t),
    triggerCandleGreen,
    higherClose,
    priceAboveSma10,
    sma10Above100,
    sma10Rising,
    sma100Rising,
    sma10:
      currentSma10 != null
        ? Number(currentSma10.toFixed(6))
        : null,
    sma100:
      currentSma100 != null
        ? Number(currentSma100.toFixed(6))
        : null,
    reason,
  };
}

function evaluateTop20DataQuality(bars) {
  const reasons = [];

  const metrics = {
    barAgeSec: null,
    duplicateBars: 0,
    zeroVolumeRatio: 0,
    malformedBars: 0,
  };

  if (
    !Array.isArray(bars) ||
    bars.length < TOP20_MIN_BARS
  ) {
    reasons.push("INSUFFICIENT_BARS");

    return {
      passed: false,
      reasons,
      metrics,
    };
  }

  let malformed = 0;
  let duplicates = 0;

  const seen = new Set();

  for (const b of bars) {
    if (
      !Number.isFinite(b.t) ||
      !Number.isFinite(b.o) ||
      !Number.isFinite(b.h) ||
      !Number.isFinite(b.l) ||
      !Number.isFinite(b.c) ||
      !Number.isFinite(b.v) ||
      b.o <= 0 ||
      b.h <= 0 ||
      b.l <= 0 ||
      b.c <= 0 ||
      b.h < b.l
    ) {
      malformed++;
    }

    if (seen.has(b.t)) {
      duplicates++;
    }

    seen.add(b.t);
  }

  metrics.malformedBars = malformed;
  metrics.duplicateBars = duplicates;

  if (malformed > 0) {
    reasons.push("MALFORMED_BARS");
  }

  if (duplicates > 0) {
    reasons.push("DUPLICATE_BARS");
  }

  const latest =
    bars[bars.length - 1];

  const barAgeSec =
    latest?.t
      ? (Date.now() - latest.t) / 1000
      : Infinity;

  metrics.barAgeSec =
    Number.isFinite(barAgeSec)
      ? Number(barAgeSec.toFixed(1))
      : null;

  if (
    !Number.isFinite(barAgeSec) ||
    barAgeSec > TOP20_MAX_BAR_AGE_SEC
  ) {
    reasons.push("STALE_DATA");
  }

  const recent =
    bars.slice(-20);

  const zeroVol =
    recent.filter(
      b => !(b.v > 0)
    ).length;

  const zeroVolumeRatio =
    recent.length
      ? zeroVol / recent.length
      : 1;

  metrics.zeroVolumeRatio =
    Number(zeroVolumeRatio.toFixed(3));

  if (
    zeroVolumeRatio >
    TOP20_MAX_ZERO_VOL_RATIO
  ) {
    reasons.push("TOO_MANY_ZERO_VOLUME_BARS");
  }

  return {
    passed: reasons.length === 0,
    reasons,
    metrics,
  };
}

function evaluateTop20Technical(leader, bars) {
  const dataQuality =
    evaluateTop20DataQuality(bars);

  if (
    !bars ||
    bars.length < TOP20_MIN_BARS
  ) {
    return {
      ...leader,
      insufficientBars: true,
      barsAvailable: bars?.length || 0,
      score: 0,
      dataQuality,
      detectedAtMs: Date.now(),
    };
  }

  const closes =
    bars.map(b => b.c);

  const macd =
    computeMacd(closes);

  const cross =
    findRecentSmaCross(
      closes,
      10,
      100,
      TOP20_CROSS_LOOKBACK
    );

  const currentSma10 =
    sma(closes, 10);

  const currentSma100 =
    sma(closes, 100);

  const pastEnd =
    closes.length -
    TOP20_SMA_TREND_LOOKBACK;

  const pastSma100 =
    sma(
      closes,
      100,
      pastEnd
    );

  const sma100SlopePct =
    currentSma100 != null &&
    pastSma100 != null &&
    pastSma100 !== 0
      ? (
          (
            currentSma100 -
            pastSma100
          ) /
          pastSma100
        ) * 100
      : 0;

  const criteria = {
    macdPositive:
      Boolean(
        macd &&
        macd.macd >
          macd.signal &&
        macd.histogram > 0
      ),

    sma10Above100:
      Boolean(
        currentSma10 != null &&
        currentSma100 != null &&
        currentSma10 >
          currentSma100
      ),

    volume:
      Number(
        leader.volume || 0
      ) >=
      TOP20_MIN_VOLUME,

    threeGreen:
      evaluateThreeGreenBullish(
        bars
      ),

    sma100Up:
      Boolean(
        currentSma100 != null &&
        pastSma100 != null &&
        currentSma100 >
          pastSma100
      ),
  };

  const bonus = {
    freshSmaCross:
      Boolean(cross.passed),
  };

  const score =
    Object.values(criteria)
      .filter(Boolean)
      .length;

  const momentumQuality =
    computeTop20MomentumQuality({
      leader,
      bars,
      macd,
      sma10: currentSma10,
      sma100: currentSma100,
      sma100Up:
        criteria.sma100Up,
    });

  const safety =
    evaluateTop20Safety({
      leader,
      bars,
      macd,
      sma10: currentSma10,
      momentumQuality,
    });

  const contradictionReasons = [];

  if (
    criteria.sma10Above100 &&
    !safety.metrics?.priceAboveSma10
  ) {
    contradictionReasons.push(
      "PRICE_BELOW_10SMA"
    );
  }

  if (
    criteria.macdPositive &&
    !momentumQuality.metrics
      ?.macdStrengthening
  ) {
    contradictionReasons.push(
      "MACD_WEAKENING"
    );
  }

  if (
    momentumQuality.metrics
      ?.redVolumeRatio >
    TOP20_MAX_RED_VOLUME_RATIO
  ) {
    contradictionReasons.push(
      "HEAVY_RED_VOLUME"
    );
  }

  if (
    safety.metrics?.recentMomentumPct <
    TOP20_MIN_RECENT_MOMENTUM_PCT
  ) {
    contradictionReasons.push(
      "RECENT_MOMENTUM_WEAK"
    );
  }

  if (!dataQuality.passed) {
    safety.passed = false;

    safety.reasons = [
      ...new Set([
        ...(safety.reasons || []),
        ...dataQuality.reasons,
      ]),
    ];
  }

  if (
    contradictionReasons.length
  ) {
    safety.passed = false;

    safety.reasons = [
      ...new Set([
        ...(safety.reasons || []),
        ...contradictionReasons,
      ]),
    ];
  }

  const detectedAtMs =
    Date.now();

  const lastCompletedBarAtMs =
    bars[bars.length - 1]?.t || 0;

  return {
    ...leader,
    score,
    criteria,
    bonus,
    momentumQuality,
    safety,
    dataQuality,
    detectedAtMs,
    lastCompletedBarAtMs,
    macdLine:
      macd?.macd ?? 0,
    macdSignal:
      macd?.signal ?? 0,
    macdHistogram:
      macd?.histogram ?? 0,
    sma10:
      currentSma10 ?? 0,
    sma100:
      currentSma100 ?? 0,
    crossBarsAgo:
      cross.barsAgo,
    sma100SlopePct,
    barsAvailable:
      bars.length,
    insufficientBars:
      false,
  };
}

function serializeTop20Result(result) {
  const c =
    result.criteria || {};

  const b =
    result.bonus || {};

  return {
    rank:
      Number(result.rank || 0),

    ticker:
      String(result.ticker || ""),

    price:
      Number(
        Number(result.price || 0)
          .toFixed(4)
      ),

    percentChange:
      Number(
        Number(result.pct || 0)
          .toFixed(2)
      ),

    volume:
      Math.round(
        Number(result.volume || 0)
      ),

    score:
      Number(result.score || 0),

    macdPositive:
      Boolean(c.macdPositive),

    sma10Above100:
      Boolean(c.sma10Above100),

    volumePass:
      Boolean(c.volume),

    threeGreen:
      Boolean(c.threeGreen),

    sma100Rising:
      Boolean(c.sma100Up),

    freshCross:
      Boolean(b.freshSmaCross),

    crossMinutesAgo:
      result.crossBarsAgo != null
        ? Number(result.crossBarsAgo)
        : null,

    earlyMomentumPassed:
      Boolean(
        result.earlyMomentum?.passed
      ),

    earlyMomentumReason:
      String(
        result.earlyMomentum?.reason ||
        "UNAVAILABLE"
      ),

    earlyVolumeMultiplier:
      Number(
        result.earlyMomentum
          ?.volumeMultiplier || 0
      ),

    earlyPriceChangePct:
      Number(
        result.earlyMomentum
          ?.priceChangePct || 0
      ),

    earlyAddedVolume:
      Number(
        result.earlyMomentum
          ?.addedVolume || 0
      ),

    earlyWindowSec:
      Number(
        result.earlyMomentum
          ?.elapsedSec || 0
      ),

    triggerPassed:
      Boolean(result.trigger?.passed),

    triggerUptrendConfirmed:
      Boolean(
        result.trigger
          ?.uptrendConfirmed
      ),

    triggerVolumeJumpPassed:
      Boolean(
        result.trigger
          ?.volumeJumpPassed
      ),

    triggerVolumeMultiplier:
      Number(
        result.trigger
          ?.multiplier || 0
      ),

    triggerCurrentCandleVolume:
      Number(
        result.trigger
          ?.currentCandleVolume || 0
      ),

    triggerPreviousCandleVolume:
      Number(
        result.trigger
          ?.previousCandleVolume || 0
      ),

    triggerCandleGreen:
      Boolean(
        result.trigger
          ?.triggerCandleGreen
      ),

    triggerHigherClose:
      Boolean(
        result.trigger
          ?.higherClose
      ),

    triggerPriceAboveSma10:
      Boolean(
        result.trigger
          ?.priceAboveSma10
      ),

    triggerSma10Above100:
      Boolean(
        result.trigger
          ?.sma10Above100
      ),

    triggerSma10Rising:
      Boolean(
        result.trigger
          ?.sma10Rising
      ),

    triggerSma100Rising:
      Boolean(
        result.trigger
          ?.sma100Rising
      ),

    triggerReason:
      String(
        result.trigger?.reason ||
        "UNAVAILABLE"
      ),

    triggerCandleAt:
      result.trigger
        ?.triggerCandleTimestampMs
        ? new Date(
            result.trigger
              .triggerCandleTimestampMs
          ).toISOString()
        : null,

    momentumQuality:
      Number(
        result.momentumQuality
          ?.score || 0
      ),

    momentumQualityLabel:
      String(
        result.momentumQuality
          ?.label ||
        "UNAVAILABLE"
      ),

    riskFlags:
      Array.isArray(
        result.momentumQuality
          ?.riskFlags
      )
        ? result.momentumQuality
            .riskFlags
        : [],

    volumeAcceleration:
      Number(
        result.momentumQuality
          ?.metrics
          ?.volumeAcceleration || 0
      ),

    distanceAboveSma10Pct:
      Number(
        result.momentumQuality
          ?.metrics
          ?.distanceAboveSma10Pct || 0
      ),

    redVolumeRatio:
      Number(
        result.momentumQuality
          ?.metrics
          ?.redVolumeRatio || 0
      ),

    recentHigh:
      Number(
        result.momentumQuality
          ?.metrics
          ?.recentHigh || 0
      ),

    nearRecentHigh:
      Boolean(
        result.momentumQuality
          ?.metrics
          ?.nearRecentHigh
      ),

    breakingRecentHigh:
      Boolean(
        result.momentumQuality
          ?.metrics
          ?.breakout
      ),

    reclaim10Sma:
      Boolean(
        result.momentumQuality
          ?.metrics
          ?.reclaim10Sma
      ),

    macdStrengthening:
      Boolean(
        result.momentumQuality
          ?.metrics
          ?.macdStrengthening
      ),

    safetyPass:
      Boolean(
        result.safety?.passed
      ),

    safetyReasons:
      Array.isArray(
        result.safety?.reasons
      )
        ? result.safety.reasons
        : [],

    priceAboveSma10:
      Boolean(
        result.safety
          ?.metrics
          ?.priceAboveSma10
      ),

    sma10SlopePct:
      Number(
        result.safety
          ?.metrics
          ?.sma10SlopePct || 0
      ),

    recentMomentumPct:
      Number(
        result.safety
          ?.metrics
          ?.recentMomentumPct || 0
      ),

    avgUpperWickRatio:
      Number(
        result.safety
          ?.metrics
          ?.avgUpperWickRatio || 0
      ),

    level1Enabled:
      Boolean(
        result.level1?.enabled
      ),

    level1Available:
      Boolean(
        result.level1?.available
      ),

    level1Pressure:
      String(
        result.level1?.pressure ||
        "UNAVAILABLE"
      ),

    level1Bullish:
      Boolean(
        result.level1?.bullish
      ),

    level1Bearish:
      Boolean(
        result.level1?.bearish
      ),

    level1Bid:
      Number(
        result.level1?.bid || 0
      ),

    level1Ask:
      Number(
        result.level1?.ask || 0
      ),

    level1BidSize:
      Number(
        result.level1?.bidSize || 0
      ),

    level1AskSize:
      Number(
        result.level1?.askSize || 0
      ),

    level1BidAskRatio:
      result.level1?.ratio != null
        ? Number(result.level1.ratio)
        : null,

    level1SpreadPct:
      result.level1?.spreadPct != null
        ? Number(result.level1.spreadPct)
        : null,

    level1QuoteAgeSec:
      result.level1?.quoteAgeSec != null
        ? Number(
            result.level1.quoteAgeSec
          )
        : null,

    dataQualityPass:
      Boolean(
        result.dataQuality?.passed
      ),

    dataQualityReasons:
      Array.isArray(
        result.dataQuality?.reasons
      )
        ? result.dataQuality.reasons
        : [],

    barAgeSec:
      result.dataQuality
        ?.metrics
        ?.barAgeSec ?? null,

    duplicateBars:
      Number(
        result.dataQuality
          ?.metrics
          ?.duplicateBars || 0
      ),

    zeroVolumeRatio:
      Number(
        result.dataQuality
          ?.metrics
          ?.zeroVolumeRatio || 0
      ),

    detectedAt:
      result.detectedAtMs
        ? new Date(
            result.detectedAtMs
          ).toISOString()
        : null,

    lastCompletedBarAt:
      result.lastCompletedBarAtMs
        ? new Date(
            result.lastCompletedBarAtMs
          ).toISOString()
        : null,

    sma10:
      Number(
        Number(result.sma10 || 0)
          .toFixed(4)
      ),

    sma100:
      Number(
        Number(result.sma100 || 0)
          .toFixed(4)
      ),

    sma100SlopePct:
      Number(
        Number(
          result.sma100SlopePct || 0
        ).toFixed(4)
      ),

    macdLine:
      Number(
        Number(
          result.macdLine || 0
        ).toFixed(6)
      ),

    macdSignal:
      Number(
        Number(
          result.macdSignal || 0
        ).toFixed(6)
      ),

    macdHistogram:
      Number(
        Number(
          result.macdHistogram || 0
        ).toFixed(6)
      ),

    barsAvailable:
      Number(
        result.barsAvailable || 0
      ),

    insufficientBars:
      Boolean(
        result.insufficientBars
      ),
  };
}

// ============================================================================
// TELEGRAM UPGRADE ALERTS
// ============================================================================

function formatTop50EarlyTelegram(result) {
  const e =
    result.earlyMomentum || {};

  const tv =
    `https://www.tradingview.com/symbols/${encodeURIComponent(
      result.ticker
    )}/`;

  return (
    `🟡 <b>TOP 50 EARLY MOMENTUM</b>\n` +
    `<b>${result.ticker}</b>  $${Number(result.price).toFixed(2)} ` +
    `(<b>${Number(result.pct).toFixed(2)}%</b>)\n` +
    `Gainer Rank: <b>#${result.rank}</b>\n` +
    `Fast price move: <b>+${Number(e.priceChangePct || 0).toFixed(2)}%</b>\n` +
    `Fast volume acceleration: <b>${Number(e.volumeMultiplier || 0).toFixed(2)}x</b>\n` +
    `Added volume: ${Number(e.addedVolume || 0).toLocaleString()}\n` +
    `Window: ${Number(e.elapsedSec || 0).toFixed(1)} sec\n\n` +
    `Watching for ≥${TOP20_TRIGGER_VOLUME_MULTIPLIER}x completed 1m confirmation.\n` +
    `<a href="${tv}">Chart →</a>`
  );
}

function formatTop50ConfirmedTelegram(result) {
  const t =
    result.trigger || {};

  const tv =
    `https://www.tradingview.com/symbols/${encodeURIComponent(
      result.ticker
    )}/`;

  return (
    `🚨 <b>TOP 50 — CONFIRMED ${TOP20_TRIGGER_VOLUME_MULTIPLIER}X VOLUME</b>\n` +
    `<b>${result.ticker}</b>  $${Number(result.price).toFixed(2)} ` +
    `(<b>${Number(result.pct).toFixed(2)}%</b>)\n` +
    `Gainer Rank: <b>#${result.rank}</b>\n\n` +
    `1m volume: <b>${Number(t.currentCandleVolume || 0).toLocaleString()}</b>\n` +
    `Previous 1m: ${Number(t.previousCandleVolume || 0).toLocaleString()}\n` +
    `Multiplier: <b>${Number(t.multiplier || 0).toFixed(2)}x</b>\n` +
    `10 SMA > 100 SMA: ${t.sma10Above100 ? "✅" : "❌"}\n` +
    `Price > 10 SMA: ${t.priceAboveSma10 ? "✅" : "❌"}\n` +
    `10 SMA rising: ${t.sma10Rising ? "✅" : "❌"}\n` +
    `100 SMA rising: ${t.sma100Rising ? "✅" : "❌"} <i>(quality only when Railway requirement=false)</i>\n\n` +
    `<a href="${tv}">Chart →</a>`
  );
}

// ============================================================================
// BASE44 / DEBUG ROUTES
// ============================================================================

app.get("/top50", (_req, res) => {
  res.set(
    "Cache-Control",
    "no-store, no-cache, must-revalidate, proxy-revalidate"
  );

  res.set(
    "Pragma",
    "no-cache"
  );

  res.set(
    "Expires",
    "0"
  );

  res.json({
    ok:
      !top20LastError ||
      top20LastResults.length > 0,

    scanner:
      "TOP 50 SCALP",

    status:
      getTop20ScannerStatus(),

    enabled:
      TOP20_ENABLED,

    isScanning:
      top20IsScanning,

    updatedAt:
      top20LastFinishedAt,

    lastStartedAt:
      top20LastStartedAt,

    lastError:
      top20LastError,

    scanIntervalMs:
      TOP20_SCAN_INTERVAL_MS,

    count:
      top20LastResults.length,

    stocks:
      top20LastResults,
  });
});

app.get("/top50_history", (_req, res) => {
  trimTop50History();

  res.json({
    ok: true,
    retentionHours:
      TOP20_SCAN_HISTORY_RETENTION_HOURS,
    count:
      top50ScanHistory.length,
    history:
      top50ScanHistory.slice(-5000),
  });
});

// ============================================================================
// NORMAL 4/5 / 5/5 ALERT STATE
// ============================================================================

const top20AlertState =
  new Map();

function shouldSendTop20Alert(result) {
  const now = Date.now();
  const ticker = result.ticker;

  const freshBonus =
    Boolean(
      result.bonus?.freshSmaCross
    );

  const state =
    top20AlertState.get(ticker) || {
      lastScore: 0,
      lastAlertScore: 0,
      lastAlertAt: 0,
      belowThresholdSince: 0,
      lastBonus: false,
    };

  const score =
    result.score;

  const safetyPass =
    result.safety?.passed !== false;

  const qualifiesForAlert =
    score >= TOP20_MIN_SCORE &&
    safetyPass;

  if (!qualifiesForAlert) {
    if (!state.belowThresholdSince) {
      state.belowThresholdSince = now;
    }

    if (
      now -
      state.belowThresholdSince >=
      TOP20_REARM_MIN * 60_000
    ) {
      state.lastAlertScore = 0;
      state.lastAlertAt = 0;
      state.lastBonus = false;
    }

    state.lastScore = score;

    top20AlertState.set(
      ticker,
      state
    );

    return false;
  }

  state.belowThresholdSince = 0;

  if (
    state.lastAlertScore <
    TOP20_MIN_SCORE
  ) {
    state.lastScore = score;
    state.lastAlertScore = score;
    state.lastAlertAt = now;
    state.lastBonus = freshBonus;

    top20AlertState.set(
      ticker,
      state
    );

    return true;
  }

  if (
    score === 5 &&
    state.lastAlertScore < 5
  ) {
    state.lastScore = score;
    state.lastAlertScore = 5;
    state.lastAlertAt = now;
    state.lastBonus = freshBonus;

    top20AlertState.set(
      ticker,
      state
    );

    return true;
  }

  if (
    freshBonus &&
    !state.lastBonus
  ) {
    state.lastScore = score;
    state.lastAlertScore =
      Math.max(
        state.lastAlertScore,
        score
      );
    state.lastAlertAt = now;
    state.lastBonus = true;

    top20AlertState.set(
      ticker,
      state
    );

    return true;
  }

  state.lastScore = score;
  state.lastBonus = freshBonus;

  top20AlertState.set(
    ticker,
    state
  );

  return false;
}

// ============================================================================
// FEED HEALTH / SHADOW
// ============================================================================

async function handleTop20FeedFailure(error) {
  top20FeedFailureCount++;

  if (
    top20FeedFailureCount >=
    TOP20_CIRCUIT_BREAKER_FAILURES
  ) {
    top20CircuitOpen = true;

    if (!top20CircuitNotified) {
      top20CircuitNotified = true;

      await pushToTelegram(
        `⚠️ <b>TOP 50 SCALP PAUSED</b>\n` +
        `Market data failed ${top20FeedFailureCount} consecutive scans.\n` +
        `Alerts are paused rather than using unreliable data.\n` +
        `Last error: ${String(
          error?.message || error
        ).slice(0, 250)}`
      );
    }
  }
}

async function handleTop20FeedSuccess() {
  const wasOpen =
    top20CircuitOpen;

  top20FeedFailureCount = 0;
  top20CircuitOpen = false;

  if (wasOpen) {
    top20LastFeedRecoveryAt =
      new Date().toISOString();

    top20CircuitNotified =
      false;

    await pushToTelegram(
      `🟢 <b>TOP 50 SCALP DATA FEED RECOVERED</b>\n` +
      `Market data is responding again. Scanner alerts resumed.`
    );
  }
}

function shouldLogShadowSignal(result) {
  if (
    !TOP20_SHADOW_MODE ||
    result.score < TOP20_MIN_SCORE ||
    result.safety?.passed
  ) {
    return false;
  }

  const key =
    result.ticker;

  const now =
    Date.now();

  const reasonsKey =
    (result.safety?.reasons || [])
      .slice()
      .sort()
      .join("|");

  const prev =
    top20ShadowState.get(key);

  if (
    !prev ||
    prev.reasonsKey !== reasonsKey ||
    now - prev.at >=
      TOP20_REARM_MIN * 60_000
  ) {
    top20ShadowState.set(
      key,
      {
        at: now,
        reasonsKey,
      }
    );

    return true;
  }

  return false;
}

// ============================================================================
// TOP-50 SCAN LOOP
// ============================================================================

async function scanTop20Technicals() {
  if (!TOP20_ENABLED) return;

  const hour =
    pacificHourNow();

  if (
    hour < TOP20_START_HOUR_PT ||
    hour >= TOP20_END_HOUR_PT
  ) {
    return;
  }

  if (top20IsScanning) {
    return;
  }

  top20IsScanning = true;
  top20ScanRuns++;

  top20LastStartedAt =
    new Date().toISOString();

  top20LastError = null;

  const started =
    Date.now();

  let leadersFetched = 0;
  let analyzed = 0;
  let insufficientBars = 0;
  let qualifying4 = 0;
  let qualifying5 = 0;
  let alertsThisRun = 0;
  let tickerErrors = 0;
  let shadowThisRun = 0;
  let earlyThisRun = 0;
  let confirmedThisRun = 0;

  try {
    const leaders =
      await fetchTop20Gainers();

    leadersFetched =
      leaders.length;

    top20LastLeaders =
      leaders.map(x => ({
        rank:
          x.rank,
        ticker:
          x.ticker,
        price:
          Number(
            x.price.toFixed(4)
          ),
        pct:
          Number(
            x.pct.toFixed(2)
          ),
        volume:
          Math.round(
            x.volume
          ),
      }));

    const evaluated =
      await runWithConcurrency(
        leaders,
        TOP20_CONCURRENCY,
        async leader => {
          try {
            const earlyMomentum =
              evaluateTop50EarlyMomentum(
                leader
              );

            const bars =
              await getTop20MinuteBars(
                leader.ticker
              );

            const trigger =
              evaluateTop20Trigger(
                bars
              );

            // IMPORTANT:
            // We no longer discard the stock just because the >=5x trigger
            // has not fired yet. Every Top-50 name gets technical analysis.
            const result =
              evaluateTop20Technical(
                leader,
                bars
              );

            if (!result) {
              return null;
            }

            result.trigger =
              trigger;

            result.earlyMomentum =
              earlyMomentum;

            if (
              trigger.multiplier >=
                TOP20_TRIGGER_DEBUG_MULTIPLIER ||
              earlyMomentum.passed ||
              trigger.passed
            ) {
              console.log(
                `[TOP50][WATCH] #${leader.rank} ${leader.ticker} ` +
                `1m=${Number(trigger.multiplier || 0).toFixed(2)}x ` +
                `current=${Number(trigger.currentCandleVolume || 0)} ` +
                `previous=${Number(trigger.previousCandleVolume || 0)} ` +
                `fast=${Number(earlyMomentum.volumeMultiplier || 0).toFixed(2)}x ` +
                `10>100=${trigger.sma10Above100} ` +
                `price>10=${trigger.priceAboveSma10} ` +
                `10up=${trigger.sma10Rising} ` +
                `100up=${trigger.sma100Rising} ` +
                `trigger=${trigger.passed ? "PASS" : trigger.reason}`
              );
            }

            pushTop50History({
              rank:
                leader.rank,
              ticker:
                leader.ticker,
              price:
                leader.price,
              pct:
                leader.pct,
              volume:
                leader.volume,

              triggerMultiplier:
                trigger.multiplier,

              triggerCurrentVolume:
                trigger.currentCandleVolume,

              triggerPreviousVolume:
                trigger.previousCandleVolume,

              triggerPassed:
                trigger.passed,

              triggerReason:
                trigger.reason,

              triggerCandleAt:
                trigger.triggerCandleTimestampMs,

              earlyPassed:
                earlyMomentum.passed,

              earlyVolumeMultiplier:
                earlyMomentum.volumeMultiplier,

              earlyPriceChangePct:
                earlyMomentum.priceChangePct,

              sma10Above100:
                trigger.sma10Above100,

              priceAboveSma10:
                trigger.priceAboveSma10,

              sma10Rising:
                trigger.sma10Rising,

              sma100Rising:
                trigger.sma100Rising,
            });

            return result;
          } catch (e) {
            tickerErrors++;

            console.error(
              `[TOP50][${leader.ticker}] error:`,
              e.message
            );

            return null;
          }
        }
      );

    if (
      leadersFetched === 0 ||
      (
        leadersFetched > 0 &&
        tickerErrors / leadersFetched > 0.5
      )
    ) {
      throw new Error(
        `Top50 feed degraded: leaders=${leadersFetched}, tickerErrors=${tickerErrors}`
      );
    }

    await handleTop20FeedSuccess();

    for (
      const result
      of evaluated.filter(Boolean)
    ) {
      if (
        result.insufficientBars
      ) {
        insufficientBars++;
        continue;
      }

      analyzed++;

      if (
        result.score === 4
      ) {
        qualifying4++;
      }

      if (
        result.score === 5
      ) {
        qualifying5++;
      }

      // EARLY alert is independent from confirmed 5x and the later 4/5-5/5.
      if (
        shouldSendTop50EarlyAlert(
          result
        )
      ) {
        await pushToTelegram(
          formatTop50EarlyTelegram(
            result
          )
        );

        earlyThisRun++;

        console.log(
          `[TOP50][EARLY] #${result.rank} ${result.ticker} ` +
          `fastVol=${Number(result.earlyMomentum?.volumeMultiplier || 0).toFixed(2)}x ` +
          `fastPrice=${Number(result.earlyMomentum?.priceChangePct || 0).toFixed(2)}%`
        );
      }

      // >=5x completed-candle confirmation is also independent.
      if (
        shouldSendTop50ConfirmedTrigger(
          result
        )
      ) {
        await pushToTelegram(
          formatTop50ConfirmedTelegram(
            result
          )
        );

        confirmedThisRun++;

        console.log(
          `[TOP50][CONFIRMED] #${result.rank} ${result.ticker} ` +
          `current=${Number(result.trigger?.currentCandleVolume || 0)} ` +
          `previous=${Number(result.trigger?.previousCandleVolume || 0)} ` +
          `multiplier=${Number(result.trigger?.multiplier || 0).toFixed(2)}x`
        );
      }

      // Level 1 stays a deeper confirmation layer for 4/5 and 5/5 setups.
      if (
        TOP20_LEVEL1_ENABLED &&
        result.score >= TOP20_MIN_SCORE
      ) {
        result.level1 =
          await getTop20Level1Quote(
            result.ticker
          );
      } else {
        result.level1 = {
          enabled:
            TOP20_LEVEL1_ENABLED,

          available:
            false,

          pressure:
            result.score >= TOP20_MIN_SCORE
              ? "UNAVAILABLE"
              : "NOT_CHECKED",

          bullish:
            false,

          bearish:
            false,

          neutral:
            true,
        };
      }

      if (
        shouldLogShadowSignal(
          result
        )
      ) {
        shadowThisRun++;
        top20ShadowSignals++;

        await recordTop20Signal(
          result,
          "SHADOW",
          result.safety?.reasons || []
        );

        console.log(
          `[TOP50][SHADOW] #${result.rank} ${result.ticker} ` +
          `score=${result.score}/5 ` +
          `blocked=${(result.safety?.reasons || []).join(",")}`
        );
      }

      const shouldAlert =
        shouldSendTop20Alert(
          result
        );

      if (
        result.score >=
          TOP20_MIN_SCORE &&
        result.safety?.passed &&
        shouldAlert
      ) {
        const telegramStartedAt =
          Date.now();

        await pushToTelegram(
          formatTop20Telegram(
            result,
            {
              scanStartedAtMs:
                started,

              detectedAtMs:
                result.detectedAtMs,
            }
          )
        );

        const telegramApiMs =
          Date.now() -
          telegramStartedAt;

        await recordTop20Signal(
          result,
          "ALERT",
          []
        );

        alertsThisRun++;
        top20AlertsSent++;

        console.log(
          `[TOP50][ALERT] #${result.rank} ${result.ticker} ` +
          `score=${result.score}/5 ` +
          `quality=${result.momentumQuality?.score || 0}/100 ` +
          `scanToSignalMs=${Math.max(0, result.detectedAtMs - started)} ` +
          `telegramApiMs=${telegramApiMs} ` +
          `pct=${result.pct.toFixed(2)} ` +
          `vol=${Math.round(result.volume)}`
        );
      }
    }

    // Publish all evaluated Top-50 names for Base44 / troubleshooting.
    top20LastResults =
      evaluated
        .filter(Boolean)
        .map(
          serializeTop20Result
        )
        .sort(
          (a, b) =>
            (b.score - a.score) ||
            (a.rank - b.rank)
        );

  } catch (e) {
    top20LastError =
      e.message;

    console.error(
      "TOP50 scan error:",
      e.message
    );

    await handleTop20FeedFailure(
      e
    );
  } finally {
    top20LastFinishedAt =
      new Date().toISOString();

    top20LastDurationMs =
      Date.now() - started;

    top20LastStats = {
      leadersFetched,
      analyzed,
      insufficientBars,
      qualifying4,
      qualifying5,
      earlyThisRun,
      confirmedThisRun,
      alertsThisRun,
      tickerErrors,
      shadowThisRun,
      circuitOpen:
        top20CircuitOpen,
    };

    top20IsScanning =
      false;
  }
}

// ============================================================================
// END TOP-50 REPLACEMENT SECTION
// YOUR EXISTING:
// // ============================================================================
// // EXISTING SCANNER
// // ============================================================================
// GOES DIRECTLY BELOW THIS.
// ============================================================================
