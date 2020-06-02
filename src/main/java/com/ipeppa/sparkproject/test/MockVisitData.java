package com.ipeppa.sparkproject.test;

import com.ipeppa.sparkproject.dao.IFakeUserInfoDataDAO;
import com.ipeppa.sparkproject.dao.IFakeVisitDataDAO;
import com.ipeppa.sparkproject.dao.impl.DAOFactory;
import com.ipeppa.sparkproject.domin.FakeUserInfoData;
import com.ipeppa.sparkproject.domin.FakeVisitData;
import com.ipeppa.sparkproject.util.DateUtils;
import com.ipeppa.sparkproject.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * “Go Further进无止境” <br>
 * 〈〉
 *
 * @author Luoxun
 * @create 2020/6/2
 * @since 1.0.0
 */
public class MockVisitData {
    public static void main(String[] args) {
        mockUserData();
    }
    public static void mockUserData() {
        Random random = new Random();
        String[] brandArray = new String[]{"huawei", "xiaomi", "oppo", "vivo", "sansumg", "htc", "apple", "jinli", "chuizi"};
        String[] sexArray = new String[]{"male", "female"};
        Integer[] proArray = new Integer[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
        List<FakeUserInfoData> fakeUserInfos = new ArrayList<FakeUserInfoData>(10000);
        for (int i = 0; i < 10; i++) {

            int s0 = random.nextInt(100) % (100 - 10 + 1) + 10;
            int s1 = random.nextInt(10000) % (10000 - 1000 + 1) + 1000;
            int s2 = random.nextInt(10000) % (10000 - 1000 + 1) + 1000;
            String userId = String.format("1%s%s%s", String.valueOf(s0), String.valueOf(s1), String.valueOf(s2));

            //用户信息
            String provinceId = String.valueOf(random.nextInt(33));
            String cityId = String.valueOf(random.nextInt(33));
            String brand = brandArray[random.nextInt(9)];
            String sex = sexArray[random.nextInt(2)];
            Integer pro = proArray[random.nextInt(12)];
            FakeUserInfoData fuid = new FakeUserInfoData();
            fuid.setMsisdn(userId);
            fuid.setProfessional(String.valueOf(pro));
            fuid.setProvinceId(provinceId);
            fuid.setAge(random.nextInt(60) % (60 - 15 + 1) + 15);
            fuid.setUserName("user".concat(userId));
            fuid.setBrand(brand);
            fuid.setSex(sex);
            fuid.setCityId(cityId);
//            IFakeUserInfoDataDAO iFakeUserInfoDataDAO = DAOFactory.getFakeUserInfoDataDAO();
//            iFakeUserInfoDataDAO.insert(fuid);
            fakeUserInfos.add(fuid);
            System.out.println(fuid.toString());
        }
        mockVisitData(fakeUserInfos);
    }

    public static void mockVisitData(List<FakeUserInfoData> fakeUserInfos) {
        String date = DateUtils.getTodayDate();
        String[] currentcatalogArray = new String[]{"content", "rank", "store", "comment"};
        Integer[] chargetypeArray = new Integer[]{0, 1};
        Integer[] sourcefromArray = new Integer[]{1, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 98, 99};
        String[] sale_parameterArray = new String[]{"0000", "0001"};
        String[] access_pointsArray = new String[]{"wlan", "cmwap", "cmnet", "unknown"};
        Integer[] logintypeArray = new Integer[]{1, 2, 3, 4};
        Integer[] visittypeArray = new Integer[]{1, 2};
        Integer[] sourcetypeArray = new Integer[]{1, 2, 3, 4, 5, 6, 7, 8, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23};
        String[] sourcesceneArray = new String[]{"from special", "from rank", "from search", "from collect", "from order"
                , "from bookmark", "from recommendMessage", "from freeMessage", "from authorWorks", "from serial", "from AIRecommend"
                , "from userMark", "from lastTime", "from paperBook", "from bookList", "from others"};
        Random random = new Random();

        List<Integer> books = new ArrayList<Integer>(1000);
        for (int i = 0; i < 1000; i++) {
            int s1 = random.nextInt(1000000) % (1000000 - 100000 + 1) + 100000;
            books.add(s1);
        }
        //模拟1000个用户
        for (int i = 0; i < fakeUserInfos.size(); i++) {
            String userId = fakeUserInfos.get(i).getMsisdn();
            int bookNum = random.nextInt(5);
            if (bookNum == 0) {
                bookNum += 1;
            }
            //每个用户模拟看1-5本书
            for (int j = 0; j < bookNum; j++) {
                String sessionId = UUID.randomUUID().toString().replace("-", "");
                int bookId = books.get(random.nextInt(1000));
                String baseActionTime = date + " " + random.nextInt(23);
                for (int k = 0; k < random.nextInt(100); k++) {
                    String actionTime = baseActionTime + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59))) + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59)));
                    String currentcatalog = currentcatalogArray[random.nextInt(4)];
                    int chargetype = chargetypeArray[random.nextInt(2)];
                    int sourcefrom = sourcefromArray[random.nextInt(15)];
                    String sale_parameter = sale_parameterArray[random.nextInt(2)];
                    String access_points = access_pointsArray[random.nextInt(4)];
                    int logintype = logintypeArray[random.nextInt(4)];
                    int visittype = visittypeArray[random.nextInt(2)];
                    int sourcetype = sourcetypeArray[random.nextInt(19)];
                    String sourcescene = sourcesceneArray[random.nextInt(16)];
                    FakeVisitData fvd = new FakeVisitData();
                    fvd.setMsisdn(userId);
                    fvd.setSessionId(sessionId);
                    fvd.setBookId(bookId);
                    fvd.setRecordTime(actionTime);
                    fvd.setCurrentCatalog(currentcatalog);
                    fvd.setChargeType(chargetype);
                    fvd.setSourceFrom(sourcefrom);
                    fvd.setSale_parameter(sale_parameter);
                    fvd.setAccess_points(access_points);
                    fvd.setLoginType(logintype);
                    fvd.setVisitType(visittype);
                    fvd.setSourceType(sourcetype);
                    fvd.setSourceScene(sourcescene);
//                    IFakeVisitDataDAO iFakeVisitDataDAO = DAOFactory.getFakeVisitDataDAO();
//                    iFakeVisitDataDAO.insert(fvd);
                    System.out.println(fvd.toString());

                }
            }


        }
    }

}
