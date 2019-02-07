package io.grisu.usvcs.rabbitmq;

public class RPCUtils {

   public static byte[] encodeMessage(String name, byte[] payload) {
      try {
         byte separator = 58;
         byte[] nServiceBytes = name.getBytes("UTF-8");

         final byte[] bytes = new byte[nServiceBytes.length + 1 + payload.length];
         System.arraycopy(nServiceBytes, 0, bytes, 0, nServiceBytes.length);
         bytes[nServiceBytes.length] = separator;
         System.arraycopy(payload, 0, bytes, nServiceBytes.length + 1, payload.length);
         return bytes;
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   public static Object[] decodeMessage(byte[] message) {
      int columnIndex = -1;
      for (int i = 0; i < message.length; i++) {
         if (message[i] == ':') {
            columnIndex = i;
            break;
         }
      }

      if (columnIndex == -1) {
         throw new RuntimeException("message malformed (missing the ':' separator)");
      }

      try {
         String name = new String(message, 0, columnIndex, "UTF-8");
         byte[] payload = new byte[message.length - columnIndex - 1];
         System.arraycopy(message, columnIndex + 1, payload, 0, payload.length);
         return new Object[] { name, payload };
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

}
