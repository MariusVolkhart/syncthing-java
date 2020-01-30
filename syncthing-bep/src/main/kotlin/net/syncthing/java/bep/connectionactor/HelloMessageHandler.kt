/*
 * Copyright (C) 2016 Davide Imbriaco
 * Copyright (C) 2018 Jonas Lochmann
 *
 * This Java file is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.syncthing.java.bep.connectionactor

import net.syncthing.java.bep.BlockExchangeProtos
import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.beans.DeviceInfo
import net.syncthing.java.core.configuration.Configuration
import net.syncthing.java.core.utils.NetworkUtils
import org.slf4j.LoggerFactory
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException

object HelloMessageHandler {
    private val logger = LoggerFactory.getLogger(HelloMessageHandler::class.java)

    fun sendHelloMessage(configuration: Configuration, outputStream: DataOutputStream) {
        sendHelloMessage(
                BlockExchangeProtos.Hello.newBuilder()
                        .setClientName(configuration.clientName)
                        .setClientVersion(configuration.clientVersion)
                        .setDeviceName(configuration.localDeviceName)
                        .build(),
                outputStream
        )
    }

    /**
     * Sends the
     * [pre-authentication message](https://docs.syncthing.net/specs/bep-v1.html#pre-authentication-messages) containing
     * the [message] to the remote client.
     *
     * @param outputStream will be flushed, but not closed.
     * @throws IOException if there is a problem writing to the [outputStream].
     */
    @Throws(IOException::class)
    internal fun sendHelloMessage(message: BlockExchangeProtos.Hello, outputStream: DataOutputStream) {
        logger.debug("Sending hello message")

        outputStream.apply {
            writeInt(ConnectionConstants.MAGIC)
            writeShort(message.serializedSize)
            message.writeTo(this)
            flush()
        }
    }

    /**
     * Receives the
     * [pre-authentication message](https://docs.syncthing.net/specs/bep-v1.html#pre-authentication-messages) from the
     * remote party.
     *
     * @param inputStream is not closed by this function.
     *
     * @throws IOException if the [inputStream] does not begin with [ConnectionConstants.MAGIC], or if the indicated
     * size of the [BlockExchangeProtos.Hello] is `0`, or if the [BlockExchangeProtos.Hello] cannot be parsed.
     */
    @Throws(IOException::class)
    fun receiveHelloMessage(inputStream: DataInputStream): BlockExchangeProtos.Hello {
        val magic = inputStream.readInt()
        NetworkUtils.assertProtocol(magic == ConnectionConstants.MAGIC) {"magic mismatch, got $magic"}

        val length = inputStream.readShort()
        NetworkUtils.assertProtocol(length > 0) {"invalid length, must be > 0, got $length"}

        return BlockExchangeProtos.Hello.parseFrom(inputStream)
    }

    suspend fun processHelloMessage(
            hello: BlockExchangeProtos.Hello,
            configuration: Configuration,
            deviceId: DeviceId
    ) {
        logger.info("Received hello message, deviceName=${hello.deviceName}, clientName=${hello.clientName}, clientVersion=${hello.clientVersion}")

        // update the local device name
        configuration.update { oldConfig ->
            oldConfig.copy(
                    peers = oldConfig.peers.map { peer ->
                        if (peer.deviceId == deviceId) {
                            DeviceInfo(deviceId, hello.deviceName)
                        } else {
                            peer
                        }
                    }.toSet()
            )
        }

        configuration.persistLater()
    }
}
