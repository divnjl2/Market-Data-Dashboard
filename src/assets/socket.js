<script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.0.1/socket.io.min.js"></script>
<script type="text/javascript">
    var socket = io.connect(window.location.origin);

    socket.on('connect', function() {
        console.log('WebSocket connected');
    });

    socket.on('log_update', function(msg) {
        let output = document.getElementById("console-output");
        output.value += msg.data;  // Добавление лога
        output.scrollTop = output.scrollHeight;  // Прокрутка к последней строке
    });
</script>
