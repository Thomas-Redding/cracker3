#!/bin/bash
# Script to inspect Polymarket catalog cache and diagnose subscription issues

CACHE_FILE="cache/polymarket_markets.jsonl"

echo "=== Polymarket Cache Inspection ==="
echo

# Check if file exists
if [ ! -f "$CACHE_FILE" ]; then
    echo "❌ ERROR: Cache file not found at $CACHE_FILE"
    echo
    echo "Solutions:"
    echo "  1. Run the bot once to trigger a cache download (may take 5-10 minutes)"
    echo "  2. Transfer the cache from another machine (see README.md)"
    exit 1
fi

# Show file size and modification time
echo "📁 Cache file: $CACHE_FILE"
ls -lh "$CACHE_FILE" | awk '{print "   Size: " $5 "  Last modified: " $6 " " $7 " " $8}'
echo

# Count total markets (first line is the "Current" entry)
TOTAL_MARKETS=$(head -1 "$CACHE_FILE" | jq '.items | length')
echo "📊 Total markets in cache: $TOTAL_MARKETS"
echo

# Check for bitcoin-related markets (case insensitive)
echo "🔍 Bitcoin-related markets:"
BTC_COUNT=$(head -1 "$CACHE_FILE" | jq '[.items[] | select(.slug // "" | ascii_downcase | contains("bitcoin"))] | length')
echo "   Total: $BTC_COUNT markets"
echo

# Check for markets matching the default pattern
PATTERN="${1:-bitcoin-above-\\d+}"
echo "🎯 Markets matching pattern '$PATTERN':"
MATCHING=$(head -1 "$CACHE_FILE" | jq --arg pattern "$PATTERN" '[.items[] | select(.slug // "" | test($pattern))] | length')
echo "   Total: $MATCHING markets"
echo

# Show some examples
echo "📋 Sample matching markets (first 10):"
head -1 "$CACHE_FILE" | jq --arg pattern "$PATTERN" '
  [.items[] | select(.slug // "" | test($pattern))] | 
  .[:10] | 
  .[] | 
  {
    slug: .slug,
    question: .question,
    end_date: .extra.end_date_iso,
    active: .extra.active,
    closed: .extra.closed
  }
' | head -60  # Limit output length
echo

# Check for expired markets
NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)
ACTIVE_COUNT=$(head -1 "$CACHE_FILE" | jq --arg now "$NOW" --arg pattern "$PATTERN" '
  [.items[] | 
   select(.slug // "" | test($pattern)) | 
   select(.extra.closed != true) |
   select(.extra.end_date_iso // "" > $now)
  ] | length
')
echo "✅ Active (non-expired) matching markets: $ACTIVE_COUNT"
echo

if [ "$ACTIVE_COUNT" -eq 0 ]; then
    echo "⚠️  WARNING: No active markets found matching the pattern!"
    echo
    echo "Suggestions:"
    echo "  1. Check your 'polymarket_pattern' in config.toml"
    echo "  2. List all bitcoin market slugs to find a better pattern:"
    echo "     head -1 $CACHE_FILE | jq '.items[] | select(.slug | test(\"bitcoin\")) | .slug' | sort | uniq"
    echo "  3. Try a broader pattern like 'bitcoin.*above' or 'btc.*\\d+'"
fi

