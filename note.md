# App 'csvtosql'
## Objective
This app is a temporary tool until the GUI app will be developed.


## Flow of process
list of files
under /logs/YYYY/transaction
*cash.csv 現金出納帳そのもの
*ja_bank.csv 預金通帳
*ja_card.csv ?
*ja_shop.csv 買掛帳
*others.csv 事業主借, 期首期末処理以外の処理
*inventory.csv 棚卸し
*mannual_op.csv

現金 <-> 預金 or 事業主借: cash.csvのみに記載

under /logs/YYYY/crops
*shipment.csv
*price.csv
*costs.csv


buying: cash, JAカード->集約（未払金）->預金, JA店->集約（買掛金）->預金, 振込
celling: 農協->集約（売掛金）->預金

cash, 振込: 直接
JAカード, JA店: 間接

Method 1
"ja_bank_buy.csv" -> "ja_bank.csv"

Method 2
if theTitle in [未払金, 買掛金] then
  tax_ratio= NULL
  ref= USJ nicos OR JA shop 明細
else // 振込
  tax_ratio != NULL
  ref= 請求書 AND 領収書
end

店での購入の流れ
(1) 店へ行く
(2) 商品をいくつかカゴに入れる
(3) レジへ行く
(4) 領収書orレシートを受け取る

lstファイルに登録
日付,時刻,店名,[I/O/T],reference file path

内訳をja_card.csv等に記載

消費税の計算式
(A) コメリ: floor(sum(p_i n r))
(B) 農協の店: sum(floor(p_i n r))


出荷関係の流れ
(1) 農協の出荷場へ出荷
(2) 市場で競りにかけられて価格決定
shipment.csvおよびprice.csv

lstファイルに登録（作物別）
日付,,集荷場,I,foo/rewards/00.png

(3) 手数料が決定
costs.csv
lstファイルに登録（作物別）
日付,,集荷場,O,foo/rewards/00.png

(4) JAバンクの口座に報酬が振り込まれる
ja_bank.csv
