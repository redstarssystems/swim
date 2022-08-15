(ns org.rssys.encrypt-test
  (:require
    [clojure.test :as test :refer [deftest testing is]]
    [matcho.core :refer [match]]
    [org.rssys.encrypt :as sut])
  (:import
    (javax.crypto
      Cipher)))



(deftest new-iv-test
  (testing "IV for AES/GCM should be `iv-length` bytes length"
    (let [iv (sut/new-iv)]
      (match (alength iv) sut/iv-length))))


(deftest gen-secret-key-test

  (testing "Generating secret key from string token is successful"
    (let [secret-token "qMkaS3^%&%@lOIOJN7h7sbrgojv"
          result       (sut/gen-secret-key secret-token)]
      (is (bytes? result) "Should be bytes array")
      (is (= 32 (alength result)) "Secret key hould be 256-bit length")))

  (testing "Secret keys from the same token are always the same"
    (let [secret-token "qMkaS3^%&%@lOIOJN7h7sqbrgojv"
          secret-bytes1 (sut/gen-secret-key secret-token)
          secret-bytes2 (sut/gen-secret-key secret-token)]
      (match (into [] secret-bytes1) (into [] secret-bytes2))))

  (testing "Secret keys from different tokens are NOT equal"
    (let [password1     "1234567890"
          password2     "123456789"
          secret-bytes1 (sut/gen-secret-key password1)
          secret-bytes2 (sut/gen-secret-key password2)]
      (is (not= (into [] secret-bytes1) (into [] secret-bytes2))))))


(deftest init-cipher-test

  (testing "Init cipher in encryption mode is successful"
    (let [cipher (sut/init-cipher (sut/gen-secret-key "123456") :encrypt (.getBytes "1234567890ab"))]
      (is (instance? Cipher cipher) "Should be Cipher instance")))

  (testing "Init cipher in decryption mode is successful"
    (let [cipher (sut/init-cipher (sut/gen-secret-key "123456") :decrypt (.getBytes "1234567890ab"))]
      (is (instance? Cipher cipher) "Should be Cipher instance")))

  (testing "Wrong cipher mode is prohibited"
    (is (thrown-with-msg? Exception #"Wrong cipher mode"
          (sut/init-cipher (sut/gen-secret-key "123456") :bad-mode (.getBytes "1234567890ab"))))))



(deftest encrypt-data-test

  (testing "Encryption is successful"
    (let [plain-text "Suppose the original message has length = 50 bytes"
          result (sut/encrypt-data (sut/gen-secret-key "123456") (.getBytes plain-text))]
      (is (bytes? result) "Should be bytes array")
      (is (> (count result) (.length plain-text)) "Encrypted bytes size should be more than plain text"))))


(deftest decrypt-test

  (testing "Decryption of encrypted text is successful"
    (let [secret-key (sut/gen-secret-key "123456")
          plain-text "Suppose the original message has length = 50 bytes"
          encrypted-bytes (sut/encrypt-data secret-key (.getBytes plain-text))
          corrupted-bytes (byte-array (update (into [] encrypted-bytes) 42 inc))
          result (String. (sut/decrypt-data secret-key encrypted-bytes))]

      (is (= plain-text result) "Original text and decrypted text should be the same")

      (testing "Corrupted data will be not decrypted"
        (is (thrown-with-msg? Exception #"Tag mismatch"
              (sut/decrypt-data secret-key corrupted-bytes)))))))

