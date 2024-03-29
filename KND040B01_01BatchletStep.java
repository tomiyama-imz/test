/**
 * プロジェクト名 ： 今治造船株式会社 TOKIUMデータ連携
 * 更新日 ： 2023/02/14
 */
package jp.co.imazo.keiri.kaikei.batch.KND040B01.KND040B01_01;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import javax.annotation.Resource;
import javax.batch.runtime.context.JobContext;
import javax.batch.runtime.context.StepContext;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;
import javax.interceptor.Interceptors;
import javax.transaction.UserTransaction;
import jp.co.imazo.common.base.ISBaseBatchletStep;
import jp.co.imazo.common.constants.Constants;
import jp.co.imazo.common.Exception.ImazoException;
import jp.co.imazo.common.Interceptor.BatHandleThrowableInterceptor;
import jp.co.imazo.common.log.BatLogging;
import jp.co.imazo.common.util.Const;
import jp.co.imazo.common.util.DateUtil;
import jp.co.imazo.common.util.MessageUtil;
import jp.co.imazo.common.util.PropertiesUtil;
import jp.co.imazo.common.util.StringUtil;
import jp.co.imazo.keiri.common.KConstants;
import jp.co.imazo.keiri.kaikei.batch.KND040B01.KND040B01_01.KND040B01_01Service.KND040B01_01ServiceDelete;
import jp.co.imazo.keiri.kaikei.batch.KND040B01.KND040B01_01.KND040B01_01Service.KND040B01_01ServiceInsert;
import jp.co.imazo.keiri.kaikei.batch.KND040B01.KND040B01_01.KND040B01_01Service.KND040B01_01ServiceRead;
import jp.co.imazo.keiri.kaikei.batch.KND040B01.KND040B01_01.KND040B01_01Service.KND040B01_01ServiceUpdate;
import jp.co.imazo.keiri.util.JsonUtil;

/**
 * KND040B01_01BatchletStep
 *
 * @description KND040ジョブステップ１のバッチ処理
 * @version 1.00 2023/02/14 新規作成
 */
@Dependent
@Named("KND040B01_01BatchletStep")
public class KND040B01_01BatchletStep extends ISBaseBatchletStep {

    @Inject
    private JobContext jobContext;
    @Inject
    private StepContext stepContext;
    @Resource
    private UserTransaction utx;

    @Inject
    private KND040B01_01ServiceRead kND040B01_01ServiceRead;
    @Inject
    private KND040B01_01ServiceDelete kND040B01_01ServiceDelete;
    @Inject
    private KND040B01_01ServiceInsert kND040B01_01ServiceInsert;
    @Inject
    private KND040B01_01ServiceUpdate kND040B01_01ServiceUpdate;

    private String strComCd;
    private String strSysDate;

    private String proxyHost;
    private String proxyPort;
    private String authString;

    // ベースURL
    // 経費精算会計データ取得API
    private static final String BASE_URL = "https://secure.keihi.com/api/v2/accounting_data_scheduled_exports/accounting_data/export?filename=";
    // インボイス会計データ取得API
    private static final String BASE_URL_INV = "https://secure.keihi.com/api/v2/payment_requests/accounting_data_scheduled_exports/accounting_data/export?filename=";

    // 出力フォルダパス
    private static final String FILE_OUT_PATH = "E:/KeiriKaikei/Data/Send/";

    // 取得ファイル名
    private static final String GET_FILE_NAME_01 = "_仮払未払_集計";
    private static final String GET_FILE_NAME_02 = "末日_現金精算_集計";
    private static final String GET_FILE_NAME_03 = "末日_振込精算_集計";
    private static final String GET_FILE_NAME_04 = "_現金精算_集計";
    private static final String GET_FILE_NAME_05 = "_振込精算_集計";
    private static final String GET_FILE_NAME_06 = "_請求書未払_集計";
    // 出力ファイル名
    private static final String OUT_FILE_NAME_01 = "KND041N00J01";
    private static final String OUT_FILE_NAME_02 = "KND041N00J02";
    private static final String OUT_FILE_NAME_03 = "KND041N00J03";
    private static final String OUT_FILE_NAME_04 = "KND041N00J04";
    private static final String OUT_FILE_NAME_05 = "KND041N00J05";
    private static final String OUT_FILE_NAME_06 = "KND041N00J06";
    // 出力拡張子(txt)
    private static final String OUTPUT_EXTENSION_TXT = ".txt";

    @Inject
    private JsonUtil jsonUtil;

    //プログラムID
    private static final String PROGRAM_ID = "KND040B01_01BatchletStep";

    //登録時 グログラムID
    private static final String BATCH_PROGRAM_ID = "KND040B01";

    /**
     * メイン処理
     * @return
     * @throws ImazoException
     */
    @Override
    @Interceptors(BatHandleThrowableInterceptor.class)
    public String processMain() throws ImazoException {

        BatLogging.logput(Constants.LOG_LEVEL_DEBUG, PROGRAM_ID, "START");

        try {
            // 初期処理
            if (init().equals(Const.JOB_STATUSCD_FAILED)) {
                stepContext.setExitStatus(Const.BAT_STATUSCD_FAILED);
                return Const.BAT_STATUSCD_FAILED;
            }

            // 仕訳データファイル取得
            getFile();

            // 終了処理
            String strEndRetCd = end();

            if(strEndRetCd.equals(Const.BAT_STATUSCD_FAILED)) {
                stepContext.setExitStatus(Const.BAT_STATUSCD_FAILED);
                return Const.BAT_STATUSCD_FAILED;
            }

            if(strEndRetCd.equals(Const.BAT_STATUSCD_MOCKCOMPLETED)) {
                stepContext.setExitStatus(Const.BAT_STATUSCD_MOCKCOMPLETED);
                return Const.BAT_STATUSCD_MOCKCOMPLETED;
            }

            stepContext.setExitStatus(Const.BAT_STATUSCD_COMPLETED);
            return Const.BAT_STATUSCD_COMPLETED;

        } catch (ImazoException e) {
            throw e;
        } catch (Exception e) {
            throw new ImazoException(e, getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName(), e.getMessage());
        } finally {
            BatLogging.logput(Constants.LOG_LEVEL_DEBUG, PROGRAM_ID, "END");
        }

    }

    /**
     * 初期処理
     * @return
     * @throws ImazoException
     */
    private String init() throws ImazoException {

        BatLogging.logput(Constants.LOG_LEVEL_DEBUG, PROGRAM_ID, "START");
        try {
            // 会社コード
            strComCd = jobContext.getProperties().getProperty(Const.JOB_CONTEXT_PARAM.concat("1"));
            BatLogging.logput(Constants.LOG_LEVEL_INFO, PROGRAM_ID, BATCH_PROGRAM_ID.concat("_会社コード：[").concat(strComCd).concat("]"));
            //会社コードの設定チェック
            if (StringUtil.chkString(strComCd).isEmpty()) {
                BatLogging.logput(Constants.LOG_LEVEL_ERROR, PROGRAM_ID, MessageUtil.getMessage("X0460E", "会社コード"));
                return Const.JOB_STATUSCD_FAILED;
            }

            // システム日付取得
            strSysDate = DateUtil.getSystemDate(KConstants.FMT_YMD);
            BatLogging.logput(Constants.LOG_LEVEL_INFO, PROGRAM_ID, BATCH_PROGRAM_ID.concat("_実行日付：[").concat(strSysDate).concat("]"));

            //プロパティファイル読み込み
            if (getProperties().equals(Const.JOB_STATUSCD_FAILED)) {
                return Const.JOB_STATUSCD_FAILED;
            }

            return Const.JOB_STATUSCD_COMPLETED;

        } catch (ImazoException e) {
            throw e;
        } catch (Exception e) {
            throw new ImazoException(e, getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName(), e.getMessage());
        } finally {
            BatLogging.logput(Constants.LOG_LEVEL_DEBUG, PROGRAM_ID, "END");
        }

    }

    /**
     * プロパティファイル読み込み
     * @return
     * @throws ImazoException
     */
    private String getProperties() throws ImazoException {

        BatLogging.logput(Constants.LOG_LEVEL_DEBUG, PROGRAM_ID, "START");
        try {
            // プロキシーサーバー経由用設定（今造環境のみ）
            proxyHost = PropertiesUtil.getProperty("K010.PROXY_HOST");
            // プロキシーサーバー経由用設定（今造環境のみ）
            proxyPort = PropertiesUtil.getProperty("K010.PROXY_PORT");
            // TOKIUMAPIキー
            authString = PropertiesUtil.getProperty("K010.AUTH_STRING.COM".concat(strComCd));
            if (StringUtil.chkString(authString).isEmpty()) {
                BatLogging.logput(Constants.LOG_LEVEL_ERROR, PROGRAM_ID, MessageUtil.getMessage("X0460E", "TOKIUMのAPIキー"));
                return Const.JOB_STATUSCD_FAILED;
            }
            jsonUtil.setProxyHost(proxyHost);
            jsonUtil.setProxyPort(proxyPort);
            jsonUtil.setAuthString(authString);

            return Const.JOB_STATUSCD_COMPLETED;

        } catch (ImazoException e) {
            throw e;
        } catch (Exception e) {
            throw new ImazoException(e, getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName(), e.getMessage());
        } finally {
            BatLogging.logput(Constants.LOG_LEVEL_DEBUG, PROGRAM_ID, "END");
        }
    }

    /**
     * 終了処理
     * @throws Exception
     */
    private String end() throws ImazoException {
        BatLogging.logput(Constants.LOG_LEVEL_DEBUG, PROGRAM_ID, "START");
        try {

            return Const.BAT_STATUSCD_COMPLETED;

        } catch (Exception e) {
            throw new ImazoException(e, getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName(), e.getMessage());
        } finally {
            BatLogging.logput(Constants.LOG_LEVEL_DEBUG, PROGRAM_ID, "END");
        }
    }

    /**
     * 仕訳データファイル取得
     * @throws ImazoException
     */
    private void getFile() throws ImazoException {

        BatLogging.logput(Constants.LOG_LEVEL_DEBUG, PROGRAM_ID, "START");
        try {
            // 実行日を取得
            String strDate = getDate("0");
            // 当日の日を取得（２桁）
            String strChkDay = strDate.substring(strDate.length() - 2);

            // 毎日作成
            downloadFile(strDate, GET_FILE_NAME_01, OUT_FILE_NAME_01);
            downloadFile(strDate, GET_FILE_NAME_06, OUT_FILE_NAME_06);
            
            
            downloadFile(strDate, GET_FILE_NAME_04, OUT_FILE_NAME_04);
            downloadFile(strDate, GET_FILE_NAME_05, OUT_FILE_NAME_05);

            // 実行日の前日を取得
            strDate = getDate("1");

            // 毎月１日のみ
            if (strChkDay.equals("01")) {
                String strYYYYMM = strDate.substring(0, 6);
                downloadFile(strYYYYMM, GET_FILE_NAME_02, OUT_FILE_NAME_02);
                downloadFile(strYYYYMM, GET_FILE_NAME_03, OUT_FILE_NAME_03);
            }

//            // 毎月１６日のみ
//            if (strChkDay.equals("16")) {
//                downloadFile(strDate, GET_FILE_NAME_04, OUT_FILE_NAME_04);
//                downloadFile(strDate, GET_FILE_NAME_05, OUT_FILE_NAME_05);
//            }

        } catch (ImazoException e) {
            throw e;
        } catch (Exception e) {
            throw new ImazoException(e, getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName(), e.getMessage());
        } finally {
            BatLogging.logput(Constants.LOG_LEVEL_DEBUG, PROGRAM_ID, "END");
        }

    }

    /**
     * データファイル取得処理
     * @param strDate
     * @param strGetFileName
     * @param strOutFileName
     * @throws IOException
     * @throws ImazoException
     */
    private void downloadFile(String strDate, String strGetFileName, String strOutFileName) throws IOException, ImazoException {

        BatLogging.logput(Constants.LOG_LEVEL_DEBUG, PROGRAM_ID, "START");
        HttpURLConnection connection = null;
        InputStream is = null;
        ByteArrayOutputStream baos = null;
        FileOutputStream fos = null;
        BufferedOutputStream bos = null;
        OutputStreamWriter osw = null;
        try {

            // プロキシー設定
            if (!proxyHost.isEmpty()) {
                System.setProperty("proxySet", "true");
                System.setProperty("proxyHost", proxyHost);
                System.setProperty("proxyPort", proxyPort);
            }

            // ＵＲＬとファイル名を編集
            // ＵＲＬ用にエンコードしたものを設定
            String strUrlName = strDate.concat(URLEncoder.encode(strGetFileName, "UTF-8"));
            String strUrl = BASE_URL.concat(strUrlName);
            // 請求書未払_集計のみ置き換え
            if (strOutFileName.equals(OUT_FILE_NAME_06)) {
                strUrl = BASE_URL_INV.concat(strUrlName);
            }
            // 出力ファイル
            String strOutFile = FILE_OUT_PATH.concat(strComCd).concat("/").concat(strOutFileName).concat(OUTPUT_EXTENSION_TXT);

            // 接続URLを設定
            URL url = new URL(strUrl);

            // URLへのコネクションを取得
            connection = (HttpURLConnection) url.openConnection();

            // HTTPのメソッドをGETに設定する
            connection.setRequestMethod("GET");
            //リクエストのボディ送信を許可しない
            connection.setDoOutput(false);
            //レスポンスのボディ受信を許可をする
            connection.setDoInput(true);

            // 認証
            if (!this.authString.isEmpty()) {
                connection.setRequestProperty("Authorization", this.authString);
            }

            // 接続タイムアウトを設定する
            connection.setConnectTimeout(100000);
            // レスポンスデータ読み取りタイムアウトを設定する
            connection.setReadTimeout(100000);
            // ヘッダーにContent-Typeを設定する
            connection.addRequestProperty("Content-Type", "text/csv");

            // コネクションを開く
            connection.connect();

            // HTTPレスポンスコード
            int intHttpstatus = connection.getResponseCode();

            if (intHttpstatus == HttpURLConnection.HTTP_OK) {
                // 通信に成功した
                System.out.println(intHttpstatus);
            } else {
                // 通信が失敗した場合のレスポンスコードを表示
                System.out.println(intHttpstatus);
                return;
            }

            // ストリームを取得
            is = connection.getInputStream();

            // 受信データをストリーム上に保管
            baos = new ByteArrayOutputStream();
            int intByte;
            while((intByte = is.read()) != -1){
                baos.write(intByte);
            }

            // ストリームをバイト配列に変換し、文字コード変換を行いながら出力
            File file = new File(strOutFile);
            fos = new FileOutputStream(file);
            bos = new BufferedOutputStream(fos);
            osw = new OutputStreamWriter(bos, "MS932");
            osw.write(new String(baos.toByteArray(), "UTF-8"));

        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new ImazoException(e, getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName(), e.getMessage());
        } finally {
            if(osw != null) {
                osw.close();
            }
            if(bos != null) {
                bos.close();
            }
            if(fos != null) {
                fos.close();
            }
            if(baos != null) {
                baos.close();
            }
            if(is != null) {
                is.close();
            }
            if(connection != null) {
                connection.disconnect();
            }
            BatLogging.logput(Constants.LOG_LEVEL_DEBUG, PROGRAM_ID, "END");
        }
    }

    /**
     * 過去日取得
     * @return
     * @throws ImazoException
     */
    private String getDate(String strMode) throws ImazoException {

        BatLogging.logput(Constants.LOG_LEVEL_DEBUG, PROGRAM_ID, "START");
        try {
            // Date型のフォーマットの設定
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

            // 現在時間の取得(Calender型)
            Calendar calendar = Calendar.getInstance();

            switch (strMode) {
                case "1":
                    // 日時を計算する
                    calendar.add(Calendar.DATE, -1);
                    break;
                default:
                    break;
            }

            // Calendar型の日時をDate型に変換
            Date d1 = calendar.getTime();
            String strDate = sdf.format(d1);

            return strDate;

        } catch (Exception e) {
            throw new ImazoException(e, getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName(), e.getMessage());
        } finally {
            BatLogging.logput(Constants.LOG_LEVEL_DEBUG, PROGRAM_ID, "END");
        }
    }

}
