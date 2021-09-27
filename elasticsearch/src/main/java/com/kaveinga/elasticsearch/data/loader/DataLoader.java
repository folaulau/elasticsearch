package com.kaveinga.elasticsearch.data.loader;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import com.kaveinga.elasticsearch.dao.CardRepository;
import com.kaveinga.elasticsearch.dao.SwipeRepository;
import com.kaveinga.elasticsearch.dao.UserRepository;
import com.kaveinga.elasticsearch.entity.Address;
import com.kaveinga.elasticsearch.entity.Card;
import com.kaveinga.elasticsearch.entity.Swipe;
import com.kaveinga.elasticsearch.entity.User;
import com.kaveinga.elasticsearch.status.UserStatus;
import com.kaveinga.elasticsearch.utils.RandomGeneratorUtils;

@Component
public class DataLoader implements CommandLineRunner {

    @Autowired
    UserRepository  userRepository;

    @Autowired
    CardRepository  cardRepository;

    @Autowired
    SwipeRepository swipeRepository;

    @Override
    public void run(String... args) throws Exception {
        // TODO Auto-generated method stub
        int numberOfUsers = 20;
        int numberOfCards = 40;
        int numberOfSwipes = numberOfCards * 5;
        double minSwipeAmount = 10;
        double maxSwipeAmount = 500;

        loadUsers(numberOfUsers);

        List<Card> cards = loadCards(numberOfCards, numberOfUsers, new ArrayList<Card>());

        loadSwipes(numberOfSwipes, cards, minSwipeAmount, maxSwipeAmount);

    }

    private void loadUsers(int numberOfUsers) {
        for (int i = 1; i <= numberOfUsers; i++) {
            User user = new User();
            user.setId(new Long(i));
            user.setDateOfBirth(LocalDate.now().minusYears(RandomGeneratorUtils.getIntegerWithin(18, 45)));

            String firstName = RandomGeneratorUtils.getRandomFirstname();
            String lastName = RandomGeneratorUtils.getRandomLastname();

            user.setEmail(RandomGeneratorUtils.getRandomEmail(firstName + lastName));
            user.setFirstName(firstName);
            user.setLastName(lastName);
            user.setDescription(RandomGeneratorUtils.getRandomAboutMe(firstName + " " + lastName));
            user.setStatus(UserStatus.getRandomStatus());
            user.setLastLoggedInAt(LocalDateTime.now().minusMinutes(RandomGeneratorUtils.getIntegerWithin(1, 3000)));

            Address primaryAddress = RandomGeneratorUtils.getRandomAddress();
            primaryAddress.setPrimary(true);
            user.addAddress(primaryAddress);
            Address secondaryAddress = RandomGeneratorUtils.getRandomAddress();
            secondaryAddress.setPrimary(false);
            user.addAddress(secondaryAddress);
            user.setRating(RandomGeneratorUtils.getIntegerWithin(1, 6));

            List<String> genders = Arrays.asList("MALE", "FEMALE");

            user.setGender(genders.get(RandomGeneratorUtils.getIntegerWithin(0, 2)));
            user.setPhoneNumber("310" + RandomGeneratorUtils.getIntegerWithin(1000000, 9999999) + "");
            userRepository.saveAndFlush(user);
        }
    }

    private List<Card> loadCards(int numberOfCards, int numberOfUsers, List<Card> cards) {

        for (int i = 1; i <= numberOfCards; i++) {
            Card card = new Card();
            card.setId(new Long(i));
            card.setExpirationDate(LocalDate.now().plusYears(new Long(4)));
            card.setUser(new User(RandomGeneratorUtils.getLongWithin(1, numberOfUsers + 1)));
            card.setCardNumber(RandomGeneratorUtils.getLongWithin(100, 999) + "" + RandomGeneratorUtils.getLongWithin(1000000, 9999999) + "");
            card.setActivatedDate(LocalDate.now().plusDays(-2));

            List<Boolean> actives = Arrays.asList(true, false);
            card.setActive(actives.get(RandomGeneratorUtils.getIntegerWithin(0, actives.size())));

            card = cardRepository.saveAndFlush(card);

            cards.add(card);
        }

        return cards;
    }

    private void loadSwipes(int numberOfSwipes, List<Card> cards, double minSwipeAmount, double maxSwipeAmount) {
        for (int i = 1; i < numberOfSwipes; i++) {
            Swipe swipe = new Swipe();
            swipe.setId(new Long(i));

            int numberOfCards = cards.size();

            Card card = cards.get(RandomGeneratorUtils.getIntegerWithin(0, numberOfCards));

            swipe.setCreatedAt(LocalDateTime.now());
            swipe.setCard(card);
            swipe.setCardNumber(card.getCardNumber());
            swipe.setAmount(RandomGeneratorUtils.getDoubleWithin(minSwipeAmount, maxSwipeAmount));

            Map<String, String> merchant = getRandomMerchant();
            swipe.setMerchantCode(merchant.get("code").toString());
            swipe.setMerchantName(merchant.get("name").toString());

            swipeRepository.saveAndFlush(swipe);
        }
    }

    private Map<String, String> getRandomMerchant() {
        List<Map<String, String>> merchants = new ArrayList<>();

        Map<String, String> merchant = new HashMap<>();
        merchant.put("code", "2831");
        merchant.put("name", "Nike");

        merchants.add(merchant);

        merchant = new HashMap<>();
        merchant.put("code", "9839");
        merchant.put("name", "Amazon");

        merchants.add(merchant);

        merchant = new HashMap<>();
        merchant.put("code", "3754");
        merchant.put("name", "Food For Less");

        merchants.add(merchant);

        merchant = new HashMap<>();
        merchant.put("code", "3859");
        merchant.put("name", "Best Buy");

        merchants.add(merchant);

        return merchants.get(RandomGeneratorUtils.getIntegerWithin(0, merchants.size()));
    }
}
