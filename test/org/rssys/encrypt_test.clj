(ns org.rssys.encrypt-test
  (:require
    [clojure.test :as test :refer [deftest testing is]]
    [matcho.core :as m :refer [match]]
    [org.rssys.encrypt :as sut])
  (:import
    (javax.crypto
      Cipher)))


(def plain-text "Suppose the original message has length = 50 bytes")

(deftest new-iv-test
  (testing "IV for AES/GCM should be `iv-length` bytes length"
    (let [iv (sut/new-iv)]
      (m/assert sut/iv-length (alength iv)))))


(deftest gen-secret-key-test

  (testing "generating secret key from string token"
    (let [secret-token "qMkaS3^%&%@lOIOJN7h7sbrgojv"
          result       (sut/gen-secret-key secret-token)]
      (is (bytes? result) "should be bytes array")
      (is (= 32 (alength result)) "secret key should be 256-bit length")))

  (testing "secret keys from the same token should always be the same"
    (let [secret-token "qMkaS3^%&%@lOIOJN7h7sqbrgojv"
          secret-bytes1 (sut/gen-secret-key secret-token)
          secret-bytes2 (sut/gen-secret-key secret-token)]
      (m/assert (into [] secret-bytes1) (into [] secret-bytes2))))

  (testing "secret keys from different tokens should be different"
    (let [password1     "1234567890"
          password2     "123456789"
          secret-bytes1 (sut/gen-secret-key password1)
          secret-bytes2 (sut/gen-secret-key password2)]
      (m/dessert (into [] secret-bytes1) (into [] secret-bytes2)))))


(deftest init-cipher-test

  (testing "encryption mode"
    (let [cipher (sut/init-cipher (sut/gen-secret-key "123456") :encrypt (.getBytes "1234567890ab"))]
      (is (instance? Cipher cipher) "should be Cipher instance")))

  (testing "decryption mode"
    (let [cipher (sut/init-cipher (sut/gen-secret-key "123456") :decrypt (.getBytes "1234567890ab"))]
      (is (instance? Cipher cipher) "should be Cipher instance")))

  (testing "wrong cipher mode is prohibited"
    (is (thrown-with-msg? Exception #"Wrong cipher mode"
          (sut/init-cipher (sut/gen-secret-key "123456") :bad-mode (.getBytes "1234567890ab"))))))



(deftest encrypt-data-test

  (testing "Encryption"
    (let [result (sut/encrypt-data (sut/gen-secret-key "123456") (.getBytes plain-text))]

      (testing "encrypted data should be bytes array"
        (m/assert bytes? result))

      (testing "encrypted data size should be more than plain text"
        (is (> (count result) (.length plain-text)))))))


(deftest decrypt-test

  (testing "Decryption"
    (let [secret-key (sut/gen-secret-key "123456")
          encrypted-bytes (sut/encrypt-data secret-key (.getBytes plain-text))
          corrupted-bytes (byte-array (update (into [] encrypted-bytes) 42 inc))
          result (String. (sut/decrypt-data secret-key encrypted-bytes))]

      (testing "decrypted text and original text should be the same"
        (m/assert plain-text result) )

      (testing "corrupted data should not decrypt"
        (is (thrown-with-msg? Exception #"Tag mismatch"
              (sut/decrypt-data secret-key corrupted-bytes)))))))

